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
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.Streams
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, BulkScrollHandler, CachedSpa, FileInfotonCaching}
import javax.inject.Inject
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.libs.json.{JsString, Json}
import play.api.mvc.{InjectedController, Request}
import security.{Admin, AuthUtils}
import wsutil.FormatterManager

import scala.concurrent.{ExecutionContext, Future}

class Auth  @Inject()(bulkScrollHandler: BulkScrollHandler,
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

  def handleAuthGet() = Action.async { req =>
    req.getQueryString("op") match {
      case Some("generate-password") => {
        val pw = authUtils.generateRandomPassword()
        Future.successful(Ok(Json.obj(("password", JsString(pw._1)), ("encrypted", pw._2))))
      }
      case Some("change-password") => {
        val currentPassword = req.getQueryString("current")
        val newPassword = req.getQueryString("new")
        val token = authUtils.extractTokenFrom(req)

        if (Seq(currentPassword, newPassword, token).forall(_.isDefined)) {
          authUtils.changePassword(token.get, currentPassword.get, newPassword.get).map {
            case true => Ok(Json.obj("success" -> true))
            case _ =>
              Forbidden(Json.obj("success" -> false, "message" -> "Current password does not match given token"))
          }
        } else {
          Future.successful(BadRequest(Json.obj("success" -> false, "message" -> "insufficient arguments")))
        }
      }
      case Some("invalidate-cache") => {
        if (isAdminEvenNonProd(req))
          authUtils.invalidateAuthCache().map(isSuccess => Ok(Json.obj("success" -> isSuccess)))
        else
          Future.successful(Unauthorized("Not authorized"))
      }
      case Some(unknownOp) =>
        Future.successful(
          BadRequest(Json.obj("success" -> false, "message" -> s"`$unknownOp` is not a valid operation"))
        )
      case None =>
        Future.successful(BadRequest(Json.obj("success" -> false, "message" -> "`op` query parameter was expected")))
    }
  }

  def isAdminEvenNonProd(r: Request[_]): Boolean = {
    authUtils.isOperationAllowedForUser(Admin, authUtils.extractTokenFrom(r), evenForNonProdEnv = true)
  }
}
