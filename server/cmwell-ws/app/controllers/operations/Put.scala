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

import java.util.Locale

import actions.ActiveInfotonGenerator
import cmwell.common.file.MimeTypeIdentifier
import cmwell.domain.{FString, FieldValue, FileContent, FileInfoton, LinkInfoton, LinkType, ObjectInfoton, SystemFields}
import cmwell.util.formats.Encoders
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.web.ld.exceptions.UnsupportedURIException
import cmwell.ws.{Settings, Streams}
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, BulkScrollHandler, CachedSpa, FileInfotonCaching, XCmWellType}
import filters.Attrs
import javax.inject.Inject
import logic.services.ServicesRoutesCache
import logic.{CRUDServiceFS, InfotonValidator}
import org.joda.time.{DateTime, DateTimeZone}
import play.api.libs.json.{JsArray, JsBoolean, JsNumber, JsObject, JsString, Json}
import play.api.mvc.request.RequestTarget
import play.api.mvc.{InjectedController, RawBuffer, Request, Result}
import play.utils.UriEncoding
import security.AuthUtils
import wsutil.{FormatterManager, asyncErrorHandler, normalizePath}

import scala.collection.mutable.{HashMap, MultiMap}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class Put @Inject()(bulkScrollHandler: BulkScrollHandler,
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

  val successResponse = Ok(Json.obj("success" -> true))

  def boolFutureToRespones(fb: Future[Boolean]) = fb.map {
    case true  => successResponse
    case false => BadRequest(Json.obj("success" -> false))
  }

  //TODO: validate query params
  def requestSlashValidator[A](request: Request[A]): Try[Request[A]] = Try {
    if (request.path.toUpperCase(Locale.ENGLISH).contains("%2F"))
      throw new UnsupportedURIException("%2F is illegal in the path part of the URI!")
    else {
      val decodedPath = UriEncoding.decodePath(request.path, "UTF-8")
      val requestHeader = request.withTarget(
        RequestTarget(uriString = decodedPath + request.uri.drop(request.path.length),
          path = decodedPath,
          queryString = request.queryString)
      )
      Request(requestHeader, request.body)
    }
  }

  private def jsonToFields(jsonBytes: Array[Byte]): Try[Map[String, Set[FieldValue]]] = {

    Try(Json.parse(jsonBytes))
      .flatMap {
        case JsObject(fields) =>
          val data: MultiMap[String, FieldValue] = new HashMap[String, collection.mutable.Set[FieldValue]]()
            with MultiMap[String, FieldValue]
          fields.foreach {
            case (k, JsString(s))  => data.addBinding(k, FString(s))
            case (k, JsNumber(n))  => data.addBinding(k, Encoders.numToFieldValue(n))
            case (k, JsBoolean(b)) => data.addBinding(k, FString("" + b))
            case (k, JsArray(arr)) =>
              arr.foreach {
                case (JsString(s))  => data.addBinding(k, FString(s))
                case (JsNumber(n))  => data.addBinding(k, Encoders.numToFieldValue(n))
                case (JsBoolean(b)) => data.addBinding(k, FString("" + b))
                case _              =>
              }
            case _ => Failure(new IllegalArgumentException("Json depth level allowed is 1"))
          }
          Success(data.map { case (x, y) => (x, y.toSet) }.toMap)
        case _ => Failure(new IllegalArgumentException("Json value must be an object"))
      }
      .recoverWith {
        case parseException =>
          Failure(new IllegalArgumentException("Request body is not a recognized json", parseException))
      }
  }

  def handlePutInfoton(path: String): Request[RawBuffer] => Future[Result] = { implicit originalRequest =>
    requestSlashValidator(originalRequest)
      .map { request =>
        val normalizedPath = normalizePath(request.path)
        val isPriorityWrite = originalRequest.getQueryString("priority").isDefined
        val modifier = request.attrs(Attrs.UserName)

        if (!InfotonValidator.isInfotonNameValid(normalizedPath)) {
          Future.successful(
            BadRequest(
              Json.obj(
                "success" -> false,
                "message" -> """you can't write to "meta" / "proc" / "ii" / or any path starting with "_" (service paths...)"""
              )
            )
          )
        } else if (isPriorityWrite && !authUtils.isOperationAllowedForUser(security.PriorityWrite,
          authUtils.extractTokenFrom(originalRequest),
          evenForNonProdEnv = true)) {
          Future.successful(
            Forbidden(Json.obj("success" -> false, "message" -> "User not authorized for priority write"))
          )
        } else
          request match {
            case XCmWellType.Object() => {
              val bodyBytes = request.body.asBytes().fold(Array.emptyByteArray)(_.toArray[Byte])
              jsonToFields(bodyBytes) match {
                case Success(fields) =>
                  InfotonValidator.validateValueSize(fields)
                  boolFutureToRespones(
                    crudServiceFS.putInfoton(ObjectInfoton(SystemFields(normalizedPath, new DateTime(DateTimeZone.UTC), modifier, Settings.dataCenter, None, "",
                      "http"), fields), isPriorityWrite)
                  )
                // TODO handle validation
                case Failure(exception) => asyncErrorHandler(exception)
              }
            }
            case XCmWellType.File() => {
              val content = request.body.asBytes().fold(Array.emptyByteArray)(_.toArray[Byte])
              if (content.isEmpty)
                Future.successful(BadRequest(Json.obj("success" -> false, "cause" -> "empty content")))
              else {
                val contentType = request.headers
                  .get("Content-Type")
                  .orElse(
                    MimeTypeIdentifier
                      .identify(content, normalizedPath.slice(normalizedPath.lastIndexOf("/"), normalizedPath.length))
                  )
                  .getOrElse("text/plain")
                boolFutureToRespones(
                  crudServiceFS.putInfoton(FileInfoton(SystemFields(path = normalizedPath, new DateTime(DateTimeZone.UTC), lastModifiedBy = modifier,
                    dc = Settings.dataCenter, None, "", "http"), content = Some(FileContent(content, contentType))), isPriorityWrite)
                )
              }
            }
            case XCmWellType.FileMD() => {
              val bodyBytes = request.body.asBytes().fold(Array.emptyByteArray)(_.toArray[Byte])
              jsonToFields(bodyBytes) match {
                case Success(fields) =>
                  InfotonValidator.validateValueSize(fields)
                  boolFutureToRespones(
                    crudServiceFS
                      .putInfoton(FileInfoton(SystemFields(normalizedPath, new DateTime(DateTimeZone.UTC), modifier, Settings.dataCenter, None, "", "http"),
                        fields = Some(fields)), isPriorityWrite)
                  )
                case Failure(exception) =>
                  Future.successful(BadRequest(Json.obj("success" -> false, "cause" -> exception.getMessage)))
              }
            }
            case XCmWellType.Link() => {
              val linkTo = request.body.asBytes().fold("")(_.utf8String)
              val linkTypeStr = request.headers.get("X-CM-WELL-LINK-TYPE").getOrElse("1")
              val linkType = linkTypeStr match {
                case "0" => LinkType.Permanent
                case "1" => LinkType.Temporary
                case "2" => LinkType.Forward
              }
              boolFutureToRespones(
                crudServiceFS.putInfoton(LinkInfoton(SystemFields(normalizedPath, new DateTime(DateTimeZone.UTC), modifier, Settings.dataCenter, None, "",
                  "http"), fields = Some(Map[String, Set[FieldValue]]()), linkTo = linkTo, linkType = linkType),
                  isPriorityWrite)
              )
            }
            case _ => Future.successful(BadRequest(Json.obj("success" -> false, "cause" -> "unrecognized type")))
          }
      }
      .recover(asyncErrorHandler)
      .get
  }
}
