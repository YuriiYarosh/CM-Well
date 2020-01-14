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
import play.api.mvc.{InjectedController, Result}
import security.AuthUtils
import wsutil.FormatterManager

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class Zz @Inject()(bulkScrollHandler: BulkScrollHandler,
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

  def handleZzGET(key: String) = Action.async { implicit req => {
    val allowed = Auth.isAdminEvenNonProd(req)

    def mapToResp(task: Future[_]): Future[Result] = {
      val p = Promise[Result]
      task.onComplete {
        case Success(_) => p.success(Ok("""{"success":true}"""))
        case Failure(e) =>
          p.success(Ok(s"""{"success":false,"message":"${Option(e.getMessage).getOrElse(e.getCause.getMessage)}"}"""))
      }
      p.future
    }

    req.getQueryString("op") match {
      case _ if !allowed => Future.successful(Forbidden("Not allowed to use zz"))
      case Some("purge") =>
        mapToResp(crudServiceFS.zStore.remove(key))
      case Some("list") =>
        (req.getQueryString("limit") match {
          case Some(limit) => crudServiceFS.zStore.ls(limit.toInt)
          case None => crudServiceFS.zStore.ls()
        }).map(lsRes => Ok(lsRes.mkString("\n")))
      case Some("put") =>
        val value = req.getQueryString("payload").getOrElse("").getBytes("UTF-8")
        mapToResp(req.getQueryString("ttl") match {
          case Some(ttl) => crudServiceFS.zStore.put(key, value, ttl.toInt, false)
          case None => crudServiceFS.zStore.put(key, value)
        })
      case None =>
        val p = Promise[Result]
        crudServiceFS.zStore.get(key).onComplete {
          case Success(payload) =>
            p.success(
              Ok(
                if (req.getQueryString("format").contains("text")) new String(payload, "UTF-8")
                else payload.mkString(",")
              )
            )
          case Failure(e) =>
            e match {
              case _: NoSuchElementException => p.success(NotFound("zz item not found"))
              case _ =>
                p.success(
                  Ok(s"""{"success":false,"message":"${Option(e.getMessage).getOrElse(e.getCause.getMessage)}"}""")
                )
            }
        }
        p.future
    }
  }
  }

  def handleZzPost(uzid: String) = Action.async(parse.raw) { implicit req =>
    val allowed = Auth.isAdminEvenNonProd(req)
    req.body.asBytes() match {
      case Some(payload) if allowed =>
        val ttl = req.getQueryString("ttl").fold(0)(_.toInt)
        crudServiceFS.zStore
          .put(uzid, payload.toArray[Byte], ttl, req.queryString.keySet("batched"))
          .map(_ => Ok("""{"success":true}"""))
          .recover {
            case e => InternalServerError(s"""{"success":false,"message":"${e.getMessage}"}""")
          }
      case None if allowed => Future.successful(BadRequest("POST body may not be empty!"))
      case _               => Future.successful(Forbidden("Not allowed to use zz"))
    }
  }
}
