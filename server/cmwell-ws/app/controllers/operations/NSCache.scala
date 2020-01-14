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

import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.util.TypeHelpers.asBoolean
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, FileInfotonCaching}
import javax.inject.Inject
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.http.ContentTypes
import play.api.libs.json.Json
import play.api.mvc.{InjectedController, Result}
import wsutil.{FormatterManager, errorHandler}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class NSCache @Inject()(crudServiceFS: CRUDServiceFS,
                        cmwellRDFHelper: CMWellRDFHelper,
                        formatterManager: FormatterManager,
                        assetsMetadataProvider: AssetsMetadataProvider,
                        assetsConfigurationProvider: AssetsConfigurationProvider,
                        servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {

  val nsCacheTimeout = akka.util.Timeout(1.minute)

  val successResponse = Ok(Json.obj("success" -> true))

  def handleNsCacheGet =
    Action.async(r => {

      def forbiddenOrElse(f: => Future[Result]) =
        if (!Auth.isAdminEvenNonProd(r))
          Future.successful(Forbidden(Json.obj("success" -> false, "message" -> "Not authorized")))
        else f

      r.getQueryString("invalidate")
        .fold {
          if (r.getQueryString("invalidate-all").fold(false)(asBoolean(_).getOrElse(true))) forbiddenOrElse {
            cmwellRDFHelper
              .invalidateAll()(nsCacheTimeout)
              .map(_ => successResponse)
              .recover(errorHandler)
          } else {
            val quick = r.getQueryString("quick").fold(false)(asBoolean(_).getOrElse(true))
            cmwellRDFHelper.newestGreatestMetaNsCacheImpl
              .getStatus(quick)(nsCacheTimeout)
              .map(resp => Ok(resp).as(ContentTypes.JSON))
          }
        } { nsID =>
          forbiddenOrElse {
            if (nsID.isEmpty)
              Future.successful(
                BadRequest(Json.obj("success" -> false, "message" -> "You need to specify whom to invalidate"))
              )
            else
              cmwellRDFHelper
                .invalidate(nsID)(nsCacheTimeout)
                .map(_ => successResponse)
                .recover(errorHandler)
          }
        }
    })
}
