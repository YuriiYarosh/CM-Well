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
import akka.util.ByteString
import cmwell.domain._
import cmwell.formats.{FormatExtractor, JsonlType}
import cmwell.syntaxutils.!!!
import cmwell.util.loading.ScalaJsRuntimeCompiler
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.{Settings, Streams}
import cmwell.ws.util.TypeHelpers.asInt
import com.typesafe.scalalogging.LazyLogging
import controllers.ApplicationUtils.getNoCacheHeaders
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, BulkScrollHandler, CachedSpa, FileInfotonCaching}
import filters.Attrs
import javax.inject.Inject
import ld.cmw.passiveFieldTypesCacheImpl
import logic.CRUDServiceFS
import logic.services.{RedirectionService, ServicesRoutesCache}
import markdown.MarkdownFormatter
import org.joda.time.DateTimeZone
import org.joda.time.format.ISODateTimeFormat
import play.api.libs.json.Json
import play.api.mvc.request.RequestTarget
import play.api.mvc.{AnyContent, InjectedController, Request, Result}
import security.AuthUtils
import wsutil._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import scala.xml.Utility

class Read @Inject()(bulkScrollHandler: BulkScrollHandler,
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

  val fullDateFormatter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC)

  def isReactive[A](req: Request[A]): Boolean = req.getQueryString("reactive").fold(false)(!_.equalsIgnoreCase("false"))

  lazy val typesCache: passiveFieldTypesCacheImpl = crudServiceFS.passiveFieldTypesCache

  def recurseRead(request: Request[AnyContent], newPath: String, recursiveCalls: Int) = handleRead(
    request.withTarget(RequestTarget(
      uriString = newPath + request.uri.drop(request.path.length),
      path = newPath,
      queryString = request.queryString)
    ).withBody(request.body),
    recursiveCalls - 1
  )

  def isMarkdown(mime: String): Boolean =
    mime.startsWith("text/x-markdown") || mime.startsWith("text/vnd.daringfireball.markdown")

  def infotonOptionToReply(request: Request[AnyContent],
                           infoton: Option[Infoton],
                           recursiveCalls: Int = 30,
                           fieldsMask: Set[String] = Set.empty): Future[Result] =
    Try {
      val offset = request.getQueryString("offset")
      val timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp)
      (if (offset.isEmpty) actions.ActiveInfotonHandler.wrapInfotonReply(infoton) else infoton) match {
        case None => Future.successful(NotFound("Infoton not found"))
        case Some(DeletedInfoton(systemFields)) =>
          Future.successful(NotFound(s"Infoton was deleted on ${fullDateFormatter.print(systemFields.lastModified)}"))
        case Some(LinkInfoton(_, _, to, lType)) =>
          lType match {
            case LinkType.Permanent => Future.successful(Redirect(to, request.queryString, MOVED_PERMANENTLY))
            case LinkType.Temporary => Future.successful(Redirect(to, request.queryString, TEMPORARY_REDIRECT))
            case LinkType.Forward if recursiveCalls > 0 => recurseRead(request, to, recursiveCalls - 1)
            case LinkType.Forward => Future.successful(BadRequest("too deep forward link chain detected!"))
          }
        case Some(i) =>
          val maskedInfoton = i.masked(fieldsMask)

          def infotonIslandResult(prefix: String, suffix: String) = {
            val infotonStr = formatterManager.getFormatter(JsonlType, timeContext).render(maskedInfoton)

            //TODO: find out why did we use "plumbing" API. we should let play determine content length etc'...
            val r = Ok(prefix + Utility.escape(infotonStr) + suffix)
              .as(overrideMimetype("text/html;charset=UTF-8", request)._2)
              .withHeaders(getNoCacheHeaders(): _*)
            //          val contentBytes = (prefix + Utility.escape(infotonStr) + suffix).getBytes("UTF-8")
            //          val r = new Result(header = ResponseHeader(200, Map(
            //            CONTENT_LENGTH -> String.valueOf(contentBytes.length), overrideMimetype("text/html;charset=UTF-8", request))
            //                              ++ getNoCacheHeaders().toMap), body = Enumerator(contentBytes))
            Future.successful(r)
          }

          //TODO: use formatter manager to get the suitable formatter
          request.getQueryString("format") match {
            case Some(FormatExtractor(formatType)) => {
              val formatter = formatterManager.getFormatter(
                format = formatType,
                timeContext = timeContext,
                host = request.host,
                uri = request.uri,
                pretty = request.queryString.keySet("pretty"),
                callback = request.getQueryString("callback"),
                offset = request.getQueryString("offset").map(_.toInt),
                length = request.getQueryString("length").map(_.toInt)
              )

              Future.successful(
                Ok(formatter.render(maskedInfoton)).as(overrideMimetype(formatter.mimetype, request)._2)
              )
            }
            // default format
            case _ =>
              i match {
                case f: FileInfoton if request.headers.get("x-compile-to").contains("js") =>
                  val scalaJsSource = new String(f.content.get.data.get, "UTF-8")
                  val mime = "application/javascript"
                  ScalaJsRuntimeCompiler
                    .compile(scalaJsSource)
                    .flatMap(
                      c => treatContentAsAsset(request, c.getBytes("UTF-8"), mime, i.systemFields.path, i.uuid, i.systemFields.lastModified)
                    )
                case f: FileInfoton => {
                  val mt = f.content.get.mimeType
                  val (content, mime) = {
                    if (isMarkdown(mt) && !request.queryString.keySet("raw")) {
                      (MarkdownFormatter.asHtmlString(f).getBytes, "text/html")
                    } else (f.content.get.data.get, mt)
                  }
                  treatContentAsAsset(request, content, mime, i.systemFields.path, i.uuid, i.systemFields.lastModified)
                }
                case c: CompoundInfoton if c.children.exists(_.systemFields.name.equalsIgnoreCase("index.html")) =>
                  Future.successful(Redirect(routes.Application.handleGET(s"${c.systemFields.path}/index.html".substring(1))))
                // ui
                case _ => {
                  val isOldUi = request.queryString.keySet("old-ui")
                  cachedSpa.getContent(isOldUi).flatMap { markup =>
                    if (markup eq null)
                      Future.successful(
                        ServiceUnavailable("System initialization was not yet completed. Please try again soon.")
                      )
                    else
                      infotonIslandResult(markup + "<inject>", "</inject>")
                  }
                }
              }
          }
      }
    }.recover(asyncErrorHandler).get


  private def handleRead(request: Request[AnyContent], recursiveCalls: Int = 30): Future[Result] =
    Try {
      val length = request.getQueryString("length").flatMap(asInt).getOrElse(0)
      val offset = request.getQueryString("offset").flatMap(asInt).getOrElse(0)
      val format = request.getQueryString("format")
      val path = normalizePath(request.path)
      val xg = request.getQueryString("xg")
      val (yg, ygChunkSize) = request
        .getQueryString("yg")
        .fold(Option.empty[String] -> 0)(
          Some(_) -> request.getQueryString("yg-chunk-size").flatMap(asInt).getOrElse(10)
        )
      val withHistory = request.queryString.keySet("with-history")
      val limit =
        request.getQueryString("versions-limit").flatMap(asInt).getOrElse(Settings.defaultLimitForHistoryVersions)
      val timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp)
      val serviceOpt = servicesRoutesCache.find(path)

      if (offset > Settings.maxOffset) {
        Future.successful(BadRequest(s"Even Google doesn't handle offsets larger than ${Settings.maxOffset}!"))
      } else if (length > Settings.maxLength) {
        Future.successful(BadRequest(s"Length is larger than ${Settings.maxOffset}!"))
      } else if (Set("/meta/ns/sys", "/meta/ns/nn")(path)) {
        Get.handleGetForActiveInfoton(request, path)
      } else if (serviceOpt.isDefined) {
        serviceOpt.get match {
          case redirection: RedirectionService if recursiveCalls > 0 =>
            val to = redirection.replaceFunc(path)
            recurseRead(request, to, recursiveCalls - 1)
          case _: RedirectionService => Future.successful(BadRequest("too deep redirection service chain detected!"))
          case _ => ???
        }
      } else {
        val reply = {
          if (withHistory && (xg.isDefined || yg.isDefined))
            Future.successful(BadRequest(s"you can't mix `xg` nor `yg` with `with-history`: it makes no sense!"))
          else if (withHistory) {
            if (isReactive(request)) {
              format.getOrElse("json") match {
                case f
                  if !Set("text", "path", "tsv", "tab", "nt", "ntriples", "nq", "nquads")(f.toLowerCase) && !f.toLowerCase
                    .startsWith("json") =>
                  Future.successful(
                    BadRequest(
                      Json.obj(
                        "success" -> false,
                        "message" -> "not a streamable type (use any json, or one of: 'text','tsv','ntriples', or 'nquads')"
                      )
                    )
                  )
                case FormatExtractor(formatType) => {
                  val formatter = formatterManager.getFormatter(
                    format = formatType,
                    timeContext = timeContext,
                    host = request.host,
                    uri = request.uri,
                    pretty = false,
                    callback = request.queryString.get("callback").flatMap(_.headOption),
                    withoutMeta = !request.queryString.keySet("with-meta"),
                    filterOutBlanks = true,
                    forceUniqueness = true
                  )

                  val infotonsSource = crudServiceFS.getInfotonHistoryReactive(path)

                  val f: Formattable => ByteString = formattableToByteString(formatter)
                  val bytes = infotonsSource.map(f.andThen {
                    _.toArray
                  })

                  Future.successful(Ok.chunked(bytes).as(overrideMimetype(formatter.mimetype, request)._2))
                }
              }
            } else {
              val formatter = format.getOrElse("atom") match {
                case FormatExtractor(formatType) =>
                  formatterManager.getFormatter(
                    format = formatType,
                    timeContext = timeContext,
                    host = request.host,
                    uri = request.uri,
                    pretty = request.queryString.keySet("pretty"),
                    callback = request.queryString.get("callback").flatMap(_.headOption),
                    withData = request.getQueryString("with-data")
                  )
              }

              crudServiceFS
                .getInfotonHistory(path, limit)
                .map(ihv => Ok(formatter.render(ihv)).as(overrideMimetype(formatter.mimetype, request)._2))
            }
          } else {
            lazy val formatter = format.getOrElse("json") match {
              case FormatExtractor(formatType) =>
                formatterManager.getFormatter(
                  formatType,
                  timeContext = timeContext,
                  request.host,
                  request.uri,
                  request.queryString.keySet("pretty"),
                  request.queryString.keySet("raw"),
                  request.queryString.get("callback").flatMap(_.headOption)
                )
              case unknown => {
                logger.warn(s"got unknown format: $unknown")
                formatterManager.prettyJsonFormatter
              }
            }

            crudServiceFS.getInfoton(path, Some(offset), Some(length)).flatMap {
              case Some(UnknownNestedContent(i)) =>
                //TODO: should still allow xg expansion?
                Future.successful(
                  PartialContent(formatter.render(i)).as(overrideMimetype(formatter.mimetype, request)._2)
                )
              case infopt => {
                val i = infopt.fold(GhostInfoton.ghost(path, "http")) {
                  case Everything(j)           => j
                  case _: UnknownNestedContent => !!!
                }
                extractFieldsMask(request, typesCache, cmwellRDFHelper, timeContext).flatMap { fieldsMask =>
                  val toRes = (f: Future[(Boolean, Seq[Infoton])]) =>
                    f.map {
                      case ((true, xs)) =>
                        Ok(formatter.render(BagOfInfotons(xs))).as(overrideMimetype(formatter.mimetype, request)._2)
                      case ((false, xs)) =>
                        InsufficientStorage(formatter.render(BagOfInfotons(xs)))
                          .as(overrideMimetype(formatter.mimetype, request)._2)
                    }
                  val ygFuncOpt = yg.map(
                    ygPattern =>
                      (iSeq: Seq[Infoton]) =>
                        pathExpansionParser(ygPattern, iSeq, ygChunkSize, cmwellRDFHelper, typesCache, timeContext)
                  )
                  val xgFuncOpt = xg.map(
                    xgPattern =>
                      (iSeq: Seq[Infoton]) => deepExpandGraph(xgPattern, iSeq, cmwellRDFHelper, typesCache, timeContext)
                  )
                  val xygFuncOpt = ygFuncOpt.flatMap(
                    ygFunc =>
                      xgFuncOpt.map(
                        xgFunc =>
                          (iSeq: Seq[Infoton]) =>
                            ygFunc(iSeq).flatMap {
                              case (true, jSeq) => xgFunc(jSeq)
                              case t            => Future.successful(t)
                            }
                      )
                  )

                  if (infopt.isEmpty && yg.isEmpty) infotonOptionToReply(request, None, recursiveCalls)
                  else
                    xygFuncOpt
                      .map(_.andThen(toRes))
                      .map(_(Seq(i)))
                      .getOrElse(
                        ygFuncOpt
                          .map(_.andThen(toRes))
                          .map(_(Seq(i)))
                          .getOrElse(
                            xgFuncOpt
                              .map(_.andThen(toRes))
                              .map(_(Seq(i)))
                              .getOrElse(infotonOptionToReply(request, Some(i), recursiveCalls, fieldsMask))
                          )
                      )
                }
              }
            }
          }
        }
        reply.recover(errorHandler)
      }
    }.recover(asyncErrorHandler).get


}
