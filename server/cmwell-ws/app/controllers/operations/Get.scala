/**
  * Copyright 2015 Thomson Reuters
  *
  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package controllers.operations

import actions.ActiveInfotonGenerator
import cmwell.domain.{Infoton, SimpleResponse, VirtualInfoton}
import cmwell.fts.FieldFilter
import cmwell.util.{BoxedFailure, EmptyBox, FullBox}
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.Streams
import cmwell.ws.util.{FieldFilterParser, FieldNameConverter}
import cmwell.ws.util.TypeHelpers.{asBoolean, asInt}
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, BulkScrollHandler, CachedSpa, FileInfotonCaching}
import filters.Attrs
import javax.inject.Inject
import ld.cmw.passiveFieldTypesCacheImpl
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.mvc.{AnyContent, InjectedController, Request}
import security.{AuthUtils, PermissionLevel}
import security.PermissionLevel.PermissionLevel
import wsutil.{FormatterManager, RawFieldFilter, asyncErrorHandler, extractFieldsMask, getFormatter, overrideMimetype}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

class Get @Inject()(bulkScrollHandler: BulkScrollHandler,
                    activeInfotonGenerator: ActiveInfotonGenerator,
                    cachedSpa: CachedSpa,
                    crudServiceFS: CRUDServiceFS,
                    streams: Streams,
                    authUtils: AuthUtils,
                    cmwellRDFHelper: CMWellRDFHelper,
                    formatterManager: FormatterManager,
                    assetsMetadataProvider: AssetsMetadataProvider,
                    assetsConfigurationProvider: AssetsConfigurationProvider,
                    servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {

  lazy val typesCache: passiveFieldTypesCacheImpl = crudServiceFS.passiveFieldTypesCache

  def handleProcGET(path: String) = Action.async { implicit req => {
    val iPath: String = {
      val noTrailingSlashes: String = path.dropTrailingChars('/')
      if (noTrailingSlashes == "/proc" || noTrailingSlashes.isEmpty) "/proc" else "/proc/" + noTrailingSlashes
    }
    handleGetForActiveInfoton(req, iPath)
  }
  }

  def getQueryString(k: String)(implicit m: Map[String, Seq[String]]): Option[String] = m.get(k).flatMap(_.headOption)

  def handleGetForActiveInfoton(req: Request[AnyContent], path: String) =
    getQueryString("qp")(req.queryString)
      .fold(Success(None): Try[Option[RawFieldFilter]])(FieldFilterParser.parseQueryParams(_).map(Some.apply))
      .map { qpOpt =>
        // todo: fix this.. should check that the token is valid...
        val tokenOpt = authUtils.extractTokenFrom(req)
        val isRoot = authUtils.isValidatedAs(tokenOpt, "root")
        val length = req
          .getQueryString("length")
          .flatMap(asInt)
          .getOrElse(if (req.getQueryString("format").isEmpty) 13 else 0) // default is 0 unless 1st request for the ajax app
        val offset = req.getQueryString("offset").flatMap(asInt).getOrElse(0)
        val withHistory = req.getQueryString("with-history").flatMap(asBoolean).getOrElse(false)
        val timeContext = req.attrs.get(Attrs.RequestReceivedTimestamp)
        val fieldsFiltersFut = qpOpt.fold[Future[Option[FieldFilter]]](Future.successful(Option.empty[FieldFilter]))(
          rff => RawFieldFilter.eval(rff, typesCache, cmwellRDFHelper, timeContext).map(Some.apply)
        )
        fieldsFiltersFut.flatMap { fieldFilters =>

          val tokenOpt = authUtils.extractTokenFrom(req)
          val isAdmin = authUtils.isOperationAllowedForUser(security.Admin, tokenOpt)

          activeInfotonGenerator
            .generateInfoton(req.host,
              path,
              req.attrs(Attrs.RequestReceivedTimestamp),
              length,
              offset,
              isRoot,
              isAdmin,
              withHistory,
              fieldFilters,
              timeContext)
            .flatMap(iOpt => Read.infotonOptionToReply(req, iOpt.map(VirtualInfoton.v2i)))
        }
      }
      .recover(asyncErrorHandler)
      .get


  def handleUuidGET(uuid: String) = Action.async { implicit req => {
    def allowed(infoton: Infoton, level: PermissionLevel = PermissionLevel.Read) =
      authUtils.filterNotAllowedPaths(Seq(infoton.systemFields.path), level, authUtils.extractTokenFrom(req)).isEmpty

    val isPurgeOp = req.getQueryString("op").contains("purge")

    def fields = req.getQueryString("fields").map(FieldNameConverter.toActualFieldNames)

    val timeContext = req.attrs.get(Attrs.RequestReceivedTimestamp)
    if (!uuid.matches("^[a-f0-9]{32}$"))
      Future.successful(BadRequest("not a valid uuid format"))
    else {
      crudServiceFS.getInfotonByUuidAsync(uuid).flatMap {
        case FullBox(infoton) if isPurgeOp && allowed(infoton, PermissionLevel.Write) =>
          val formatter = getFormatter(req, formatterManager, "json")

          req.getQueryString("index") match {
            case Some(index) =>
              crudServiceFS
                .purgeUuidFromIndex(uuid, index)
                .map(
                  _ =>
                    Ok(
                      formatter.render(
                        SimpleResponse(success = true,
                          Some(s"Note: $uuid was only purged from $index but not from CAS!"))
                      )
                    ).as(overrideMimetype(formatter.mimetype, req)._2)
                )
            case None =>
              crudServiceFS.getInfotons(Seq(infoton.systemFields.path)).flatMap { boi =>
                if (infoton.uuid == boi.infotons.head.uuid)
                  Future.successful(
                    BadRequest(
                      "This specific version of CM-Well does not support this operation for the last version of the Infoton."
                    )
                  )
                else {
                  crudServiceFS.purgeUuid(infoton).map { _ =>
                    Ok(formatter.render(SimpleResponse(success = true, None)))
                      .as(overrideMimetype(formatter.mimetype, req)._2)
                  }
                }
              }
          }
        case FullBox(infoton) if isPurgeOp => Future.successful(Forbidden("Not authorized"))

        case FullBox(infoton) if allowed(infoton) =>
          extractFieldsMask(req, typesCache, cmwellRDFHelper, timeContext)
            .flatMap(fm => infotonOptionToReply(req, Some(infoton), fieldsMask = fm))
        case FullBox(infoton) => Future.successful(Forbidden("Not authorized"))

        case EmptyBox => infotonOptionToReply(req, None)
        case BoxedFailure(e) => asyncErrorHandler(e)
      }
    }
  }
  }
}
