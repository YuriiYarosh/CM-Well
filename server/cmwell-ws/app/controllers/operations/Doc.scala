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

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cmwell.util.{BoxedFailure, EmptyBox, FullBox}
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.Settings
import cmwell.ws.util.TypeHelpers.asInt
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, FileInfotonCaching}
import javax.inject.Inject
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.libs.json.{JsNull, JsNumber, Json}
import play.api.mvc.InjectedController
import wsutil.{FormatterManager, asyncErrorHandler, dtf, endln, overrideMimetype}

import scala.concurrent.{ExecutionContext, Future}

class Doc @Inject()(crudServiceFS: CRUDServiceFS,
                    cmwellRDFHelper: CMWellRDFHelper,
                    formatterManager: FormatterManager,
                    assetsMetadataProvider: AssetsMetadataProvider,
                    assetsConfigurationProvider: AssetsConfigurationProvider,
                    servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {
  val pathHeader = ByteString("path,last_modified,uuid")
  val pathHeaderSource: Source[ByteString, NotUsed] = Source.single(pathHeader)

  val commaByteString = ByteString(",", StandardCharsets.UTF_8)

  def handleRawPath(path: String, isReactive: Boolean, limit: Int)(
    defaultMimetypeToReturnedMimetype: String => String
  ) = {
    val pathByteString = ByteString(path, StandardCharsets.UTF_8)
    if (isReactive) {
      val src = crudServiceFS
        .getRawPathHistoryReactive(path)
        .map {
          case (time, uuid) =>
            endln ++
              pathByteString ++
              commaByteString ++
              ByteString(dtf.print(time), StandardCharsets.UTF_8) ++
              commaByteString ++
              ByteString(uuid, StandardCharsets.UTF_8)
        }
      Future.successful(
        Ok.chunked(pathHeaderSource.concat(src)).as(defaultMimetypeToReturnedMimetype("text/csv;charset=UTF-8"))
      )
    } else
      crudServiceFS.getRawPathHistory(path, limit).map { vec =>
        val payload = vec.foldLeft(pathHeader) {
          case (bytes, (time, uuid)) =>
            bytes ++
              endln ++
              pathByteString ++
              commaByteString ++
              ByteString(dtf.print(time), StandardCharsets.UTF_8) ++
              commaByteString ++
              ByteString(uuid, StandardCharsets.UTF_8)
        }
        Ok(payload).as(defaultMimetypeToReturnedMimetype("text/csv;charset=UTF-8"))
      }
  }

  def handleRawUUID(uuid: String, isReactive: Boolean, limit: Int)(
    defaultMimetypeToReturnedMimetype: String => String
  ) = {
    if (isReactive) {
      val src = crudServiceFS.reactiveRawCassandra(uuid).intersperse("\n").map(ByteString(_, StandardCharsets.UTF_8))
      Future.successful(Ok.chunked(src).as(defaultMimetypeToReturnedMimetype("text/csv;charset=UTF-8")))
    } else
      crudServiceFS.getRawCassandra(uuid).flatMap {
        case (payload, _) if payload.lines.size < 2 =>
          handleRawPath("/" + uuid, isReactive, limit)(defaultMimetypeToReturnedMimetype)
        case (payload, mimetype) => Future.successful(Ok(payload).as(defaultMimetypeToReturnedMimetype(mimetype)))
      }
  }

  def handleRawDoc(uuid: String) = Action.async { req =>
    crudServiceFS
      .getInfotonByUuidAsync(uuid)
      .flatMap {
        case FullBox(i) => {
          val index = i.systemFields.indexName
          val a = handleRawDocWithIndex(index, uuid)
          a(req)
        }
        case BoxedFailure(ex) => logger.error(s"Unexpected result: Boxed Failiure with exception:", ex) ; throw ex
        case EmptyBox => logger.error(s"Unexpected result: Empty Boxed") ; ???
      }
      .recoverWith {
        case err: Throwable => {
          logger.error(s"could not retrive uuid[$uuid] from cassandra", err)
          crudServiceFS.ftsService.uinfo(uuid).map { vec =>
            val jArr = vec.map {
              case (index, version, source) =>
                Json.obj("_index" -> index, "_version" -> JsNumber(version), "_source" -> Json.parse(source))
            }

            val j = {
              if (jArr.length < 2) jArr.headOption.getOrElse(JsNull)
              else Json.arr(jArr)
            }
            val pretty = req.queryString.keySet("pretty")
            val payload = {
              if (pretty) Json.prettyPrint(j)
              else Json.stringify(j)
            }
            Ok(payload).as(overrideMimetype("application/json;charset=UTF-8", req)._2)
          }
        }
      }
      .recoverWith(asyncErrorHandler)
  }

  def handleRawDocWithIndex(index: String, uuid: String) = Action.async { req =>
    crudServiceFS.ftsService
      .extractSource(uuid, index)
      .map {
        case (source, version) =>
          val j = Json.obj("_index" -> index, "_version" -> JsNumber(version), "_source" -> Json.parse(source))
          val pretty = req.queryString.keySet("pretty")
          val payload = {
            if (pretty) Json.prettyPrint(j)
            else Json.stringify(j)
          }
          Ok(payload).as(overrideMimetype("application/json;charset=UTF-8", req)._2)
      }
      .recoverWith(asyncErrorHandler)
  }

  def handleRawRow(path: String) = Action.async { req =>
    val limit = req.getQueryString("versions-limit").flatMap(asInt).getOrElse(Settings.defaultLimitForHistoryVersions)
    path match {
      case Uuid(uuid) =>
        handleRawUUID(uuid, Read.isReactive(req), limit) { defaultMimetype =>
          overrideMimetype(defaultMimetype, req)._2
        }.recoverWith(asyncErrorHandler)
      case _ => {
        handleRawPath("/" + path, isReactive(req), limit) { defaultMimetype =>
          overrideMimetype(defaultMimetype, req)._2
        }.recoverWith(asyncErrorHandler)
      }
    }
  }
}
