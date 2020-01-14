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
import cmwell.domain.SimpleResponse
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.{Settings, Streams}
import cmwell.ws.util.TypeHelpers.asInt
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, BulkScrollHandler, CachedSpa, FileInfotonCaching}
import javax.inject.Inject
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.mvc.{AnyContent, InjectedController, Request, Result}
import security.{AuthUtils, PermissionLevel}
import wsutil.{FormatterManager, getFormatter, normalizePath, overrideMimetype}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class Purge @Inject()(bulkScrollHandler: BulkScrollHandler,
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

  def handlePurgeAll(req: Request[AnyContent]) = handlePurge(req, includeLast = true)

  def handlePurgeHistory(req: Request[AnyContent]) = handlePurge(req, includeLast = false)

  def handlePurgeLast(req: Request[AnyContent]) = handlePurge(req, includeLast = true, onlyLast = true)

  private def handlePurge(req: Request[AnyContent], includeLast: Boolean, onlyLast: Boolean = false): Future[Result] = {
    if (req.getQueryString("2").isDefined) handlePurge2(req)
    else {

      lazy val notImplemented =
        Future.failed(new NotImplementedError("This specific version of CM-Well does not support this operation."))

      val p = Promise[Result]()

      val path = normalizePath(req.path)
      val allowed =
        authUtils.filterNotAllowedPaths(Seq(path), PermissionLevel.Write, authUtils.extractTokenFrom(req)).isEmpty
      if (!allowed) {
        p.completeWith(Future.successful(Forbidden("Not authorized")))
      } else if (path == "/") {
        p.completeWith(Future.successful(BadRequest("Purging Root Infoton does not make sense!")))
      } else {
        val formatter = getFormatter(req, formatterManager, "json")
        val limit =
          req.getQueryString("versions-limit").flatMap(asInt).getOrElse(Settings.defaultLimitForHistoryVersions)
        val res =
          if (onlyLast)
            notImplemented // CRUDServiceFS.rollback(path,limit)
          else if (includeLast) crudServiceFS.purgePath(path, includeLast, limit)
          else notImplemented // CRUDServiceFS.purgePath(path, includeLast, limit)

        res.onComplete {
          case Success(_) =>
            p.completeWith(
              Future.successful(
                Ok(formatter.render(SimpleResponse(true, None))).as(overrideMimetype(formatter.mimetype, req)._2)
              )
            )
          case Failure(e) =>
            p.completeWith(
              Future.successful(
                Ok(formatter.render(SimpleResponse(false, Option(e.getCause.getMessage))))
                  .as(overrideMimetype(formatter.mimetype, req)._2)
              )
            )
        }
      }

      p.future
    }
  }

  private def handlePurge2(req: Request[AnyContent]): Future[Result] = {
    val path = normalizePath(req.path)
    val allowed =
      authUtils.filterNotAllowedPaths(Seq(path), PermissionLevel.Write, authUtils.extractTokenFrom(req)).isEmpty
    if (!allowed) {
      Future.successful(Forbidden("Not authorized"))
    } else {
      val formatter = getFormatter(req, formatterManager, "json")
      val limit = req.getQueryString("versions-limit").flatMap(asInt).getOrElse(Settings.defaultLimitForHistoryVersions)
      crudServiceFS.purgePath2(path, limit).map { b =>
        Ok(formatter.render(SimpleResponse(true, None))).as(overrideMimetype(formatter.mimetype, req)._2)
      }
    }
  }

}
