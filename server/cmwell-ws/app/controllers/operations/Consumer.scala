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

import cmwell.fts.FieldFilter
import cmwell.util.concurrent.SimpleScheduler
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.adt.{ConsumeState, SortedConsumeState}
import cmwell.ws.util.FieldFilterParser
import cmwell.ws.util.TypeHelpers.asLong
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, FileInfotonCaching}
import filters.Attrs
import javax.inject.Inject
import ld.cmw.passiveFieldTypesCacheImpl
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.libs.json.Json
import play.api.mvc.{AnyContent, InjectedController, Request, Result}
import wsutil.{FormatterManager, RawFieldFilter, asyncErrorHandler, errorHandler, normalizePath}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class Consumer @Inject()(crudServiceFS: CRUDServiceFS,
                         cmwellRDFHelper: CMWellRDFHelper,
                         formatterManager: FormatterManager,
                         assetsMetadataProvider: AssetsMetadataProvider,
                         assetsConfigurationProvider: AssetsConfigurationProvider,
                         servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {

  lazy val typesCache: passiveFieldTypesCacheImpl = crudServiceFS.passiveFieldTypesCache

  def generateSortedConsumeFieldFilters(qpOpt: Option[String],
                                        path: String,
                                        withDescendants: Boolean,
                                        withHistory: Boolean,
                                        withDeleted: Boolean,
                                        indexTime: Long,
                                        timeContext: Option[Long]): Future[SortedConsumeState] = {
    val pOpt = {
      if (path == "/" && withDescendants) None
      else Some(path)
    }

    qpOpt
      .fold[Future[Option[FieldFilter]]](Future.successful(None)) { qp =>
        FieldFilterParser.parseQueryParams(qp) match {
          case Failure(err) => Future.failed(err)
          case Success(rff) => RawFieldFilter.eval(rff, typesCache, cmwellRDFHelper, timeContext).map(Some.apply)
        }
      }
      .map { ffOpt =>
        SortedConsumeState(indexTime, pOpt, withHistory, withDeleted, withDescendants, ffOpt)
      }
  }

  private def handleCreateConsumerRequest(request: Request[AnyContent]): Future[Result] =
    handleCreateConsumer(request.path, request.attrs.get(Attrs.RequestReceivedTimestamp))(request.queryString)

  private def handleCreateConsumer(path: String, requestReceivedTimestamp: Option[Long])(
    createConsumerParams: Map[String, Seq[String]]
  ): Future[Result] =
    Try {
      val indexTime = createConsumerParams.get("index-time").flatMap(_.headOption.flatMap(asLong))
      val normalizedPath = normalizePath(path)
      val qpOpt = createConsumerParams.get("qp").flatMap(_.headOption)
      val withDescendants = createConsumerParams.contains("with-descendants") || createConsumerParams.contains(
        "recursive"
      )
      val withHistory = createConsumerParams.contains("with-history")
      val withDeleted = createConsumerParams.contains("with-deleted")
      val lengthHint = createConsumerParams.get("length-hint").flatMap(_.headOption.flatMap(asLong))
      val consumeStateFut: Future[ConsumeState] = {
        val f = generateSortedConsumeFieldFilters(qpOpt,
          normalizedPath,
          withDescendants,
          withHistory,
          withDeleted,
          indexTime.getOrElse(0L),
          requestReceivedTimestamp)
        lengthHint.fold[Future[ConsumeState]](f)(lh => f.map(_.asBulk(lh)))
      }
      consumeStateFut
        .transformWith {
          case Failure(err) => {
            val msg = s"failed to evaluate given qp [${qpOpt.fold("")(_.toString)}]"
            logger.error(msg, err)
            val res = FailedDependency(Json.obj("success" -> false, "message" -> msg))
            requestReceivedTimestamp.fold(Future.successful(res)) { reqStartTime =>
              val timePassedInMillis = System.currentTimeMillis() - reqStartTime
              if (timePassedInMillis > 9000L) Future.successful(res)
              else SimpleScheduler.schedule((9500L - timePassedInMillis).millis)(res)
            }
          }
          case Success(scs) => {
            val id = ConsumeState.encode(scs)
            Future.successful(Ok("").withHeaders("X-CM-WELL-POSITION" -> id))
          }
        }
        .recover(errorHandler)
    }.recover(asyncErrorHandler).get
}
