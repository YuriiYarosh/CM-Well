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
import akka.pattern.AskTimeoutException
import cmwell.domain.BagOfInfotons
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.Streams
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, BulkScrollHandler, CachedSpa, FileInfotonCaching}
import javax.inject.Inject
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.mvc.{Action, AnyContent, InjectedController}
import security.AuthUtils
import wsutil.{FormatterManager, getFormatter, overrideMimetype, pathStatusAsInfoton}

import scala.concurrent.{ExecutionContext, Future}

class Track @Inject()(bulkScrollHandler: BulkScrollHandler,
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

  def handleTrack(trackingId: String): Action[AnyContent] = Action.async { implicit request =>
    import cmwell.tracking.{InProgress, TrackingId, TrackingUtil}

    def getDataFromActor(trackingId: String) = trackingId match {
      case TrackingId(tid) => logger.debug(s"Tracking: Trying to get data from $tid"); TrackingUtil().readStatus(tid)
      case _ => Future.failed(new IllegalArgumentException(s"Invalid trackingID"))
    }

    val resultsFut = getDataFromActor(trackingId)
    val formatter = getFormatter(request, formatterManager, defaultFormat = "ntriples", withoutMeta = true)

    def errMsg(msg: String) = s"""{"success":false,"error":"$msg"}"""

    resultsFut
      .map { results =>
        val trackingHeaders =
          if (results.forall(_.status != InProgress)) Seq("X-CM-WELL-TRACKING" -> "Done")
          else Seq.empty[(String, String)]
        val response = BagOfInfotons(results.map(pathStatusAsInfoton))
        Ok(formatter.render(response))
          .withHeaders(trackingHeaders: _*)
          .as(overrideMimetype(formatter.mimetype, request)._2)
      }
      .recover {
        // not using errorHandler in order to hide from user the exception message.
        case _: IllegalArgumentException => BadRequest(errMsg("Invalid trackingID"))
        case _: NoSuchElementException =>
          Gone(errMsg("This tracking ID was never created or its tracked request has been already completed"))
        case _: AskTimeoutException => ServiceUnavailable(errMsg("Tracking is currently unavailable"))
        case _ => InternalServerError(errMsg("An unexpected error has occurred"))
      }
  }
}
