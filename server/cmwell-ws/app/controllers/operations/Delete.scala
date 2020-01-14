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

import cmwell.domain.{FString, FieldValue, SearchResults}
import cmwell.fts.{DatesFilter, PaginationParams, PathFilter, SortParam}
import cmwell.web.ld.cmw.CMWellRDFHelper
import com.typesafe.scalalogging.LazyLogging
import controllers.ApplicationUtils.infotonPathDeletionAllowed
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, FileInfotonCaching}
import filters.Attrs
import javax.inject.Inject
import logic.services.ServicesRoutesCache
import logic.{CRUDServiceFS, InfotonValidator}
import play.api.libs.json.Json
import play.api.mvc.{InjectedController, Result}
import wsutil.{FormatterManager, asyncErrorHandler, normalizePath}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class Delete @Inject()(crudServiceFS: CRUDServiceFS,
                       cmwellRDFHelper: CMWellRDFHelper,
                       formatterManager: FormatterManager,
                       assetsMetadataProvider: AssetsMetadataProvider,
                       assetsConfigurationProvider: AssetsConfigurationProvider,
                       servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {
  /**
    * returns a boolean future determining weather deletion is allowed, and if it's forbidden, also returns a message with the reason.
    *
    * @param path
    * @return
    */
  def infotonPathDeletionAllowed(path: String, recursive: Boolean, crudServiceFS: CRUDServiceFS)(
    implicit ec: ExecutionContext
  ): Future[Either[String, Seq[String]]] = {
    def numOfChildren(p: String): Future[SearchResults] = {
      val pathFilter = if (p.length > 1) Some(PathFilter(p, true)) else None
      crudServiceFS.search(pathFilter,
        None,
        Some(DatesFilter(None, None)),
        PaginationParams(0, 500),
        false,
        false,
        SortParam.empty,
        false,
        false)
    }

    if (!InfotonValidator.isInfotonNameValid(path)) Future.successful {
      Left(s"the path $path is not valid for deletion. ")
    } else if (recursive) numOfChildren(path).map {
      case sr if sr.total > 500 =>
        Left("recursive DELETE is forbidden for large (>500 descendants) infotons. DELETE descendants first.")
      case sr => Right(sr.infotons.map(_.systemFields.path) :+ path)
    } else Future.successful(Right(Seq(path)))
  }

  def handleDELETE(path: String) = Action.async(parse.raw) { implicit originalRequest =>
    Put.requestSlashValidator(originalRequest) match {
      case Failure(e) => asyncErrorHandler(e)
      case Success(request) => {
        val normalizedPath = normalizePath(request.path)
        val modifier = request.attrs(Attrs.UserName)
        val isPriorityWrite = originalRequest.getQueryString("priority").isDefined
        if (!InfotonValidator.isInfotonNameValid(normalizedPath))
          Future.successful(
            BadRequest(
              Json.obj(
                "success" -> false,
                "message" -> """you can't delete from "proc" / "ii" / or any path starting with "_" (service paths...)"""
              )
            )
          )
        else if (isPriorityWrite && !authUtils.isOperationAllowedForUser(security.PriorityWrite,
          authUtils.extractTokenFrom(originalRequest),
          evenForNonProdEnv = true))
          Future.successful(
            Forbidden(Json.obj("success" -> false, "message" -> "User not authorized for priority write"))
          )
        else {
          //deleting values based on json
          request.getQueryString("data") match {
            case Some(jsonStr) =>
              jsonToFields(jsonStr.getBytes("UTF-8")) match {
                case Success(fields) =>
                  crudServiceFS.deleteInfoton(normalizedPath, modifier, Some(fields), isPriorityWrite).map { _ =>
                    Ok(Json.obj("success" -> true))
                  }
                case Failure(exception) => asyncErrorHandler(exception)
              }
            case None => {
              val fields: Option[Map[String, Set[FieldValue]]] = request.getQueryString("field") match {
                case Some(field) =>
                  val value = request.getQueryString("value") match {
                    case Some(value) => FString(value)
                    case _ => FString("*")
                  }
                  Some(Map(field -> Set(value)))
                case _ => None
              }

              val p = Promise[Result]()
              for {
                either <- infotonPathDeletionAllowed(normalizedPath,
                  request.getQueryString("recursive").getOrElse("true").toBoolean,
                  crudServiceFS)
              } {
                (fields.isDefined, either) match {
                  case (true, _) =>
                    crudServiceFS
                      .deleteInfoton(normalizedPath, modifier, fields, isPriorityWrite)
                      .onComplete {
                        case Success(b) => p.success(Ok(Json.obj("success" -> b)))
                        case Failure(e) =>
                          p.success(InternalServerError(Json.obj("success" -> false, "message" -> e.getMessage)))
                      }
                  case (false, Right(paths)) =>
                    crudServiceFS
                      .deleteInfotons(paths.map((_, None)).toList, modifier, isPriorityWrite = isPriorityWrite)
                      .onComplete {
                        case Success(b) => p.success(Ok(Json.obj("success" -> b)))
                        case Failure(e) =>
                          p.success(InternalServerError(Json.obj("success" -> false, "message" -> e.getMessage)))
                      }
                  case (false, Left(msg)) => p.success(BadRequest(Json.obj("success" -> false, "message" -> msg)))
                }
              }
              p.future
            }
          }
        }
      }
    }
  }
}
