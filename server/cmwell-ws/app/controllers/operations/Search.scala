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

import java.net.URLEncoder

import actions.ActiveInfotonGenerator
import cmwell.domain.{PaginationInfo, SearchResponse}
import cmwell.formats.FormatExtractor
import cmwell.fts.{DatesFilter, FieldFilter, PaginationParams, PathFilter}
import cmwell.util.concurrent.SimpleScheduler
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.{Settings, Streams}
import cmwell.ws.util.RequestHelpers.cmWellBase
import cmwell.ws.util.{DateParser, FieldFilterParser, FromDate, SortByParser, ToDate}
import cmwell.ws.util.TypeHelpers.asInt
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, BulkScrollHandler, CachedSpa, FileInfotonCaching}
import filters.Attrs
import javax.inject.Inject
import ld.cmw.passiveFieldTypesCacheImpl
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.libs.json.Json
import play.api.mvc.{AnyContent, InjectedController, Request, Result}
import security.AuthUtils
import wsutil._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class Search @Inject()(bulkScrollHandler: BulkScrollHandler,
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


  lazy val typesCache: passiveFieldTypesCacheImpl = crudServiceFS.passiveFieldTypesCache

  private def handleSearch(r: Request[AnyContent]): Future[Result] =
    handleSearch(normalizePath(r.path), cmWellBase(r), r.host, r.uri, r.attrs.get(Attrs.RequestReceivedTimestamp))(
      r.queryString
    )

  private def handleSearch(
                            normalizedPath: String,
                            cmWellBase: String,
                            requestHost: String,
                            requestUri: String,
                            requestReceivedTimestamp: Option[Long]
                          )(implicit queryString: Map[String, Seq[String]]): Future[Result] =
    Get.getQueryString("qp")
      .fold(Success(None): Try[Option[RawFieldFilter]])(FieldFilterParser.parseQueryParams(_).map(Some.apply))
      .map { qpOpt =>
        val from = DateParser.parseDate(Get.getQueryString("from").getOrElse(""), FromDate).toOption
        val to = DateParser.parseDate(Get.getQueryString("to").getOrElse(""), ToDate).toOption
        val length = Get.getQueryString("length").flatMap(asInt).getOrElse(10)
        val offset = Get.getQueryString("offset").flatMap(asInt).getOrElse(0)
        val withDataFormat = Get.getQueryString("with-data")
        val withData = withDataFormat.isDefined && withDataFormat.get.toLowerCase != "false"
        //FIXME: `getOrElse` swallows parsing errors that should come out as `BadRequest`
        val rawSortParams =
          Get.getQueryString("sort-by").flatMap(SortByParser.parseFieldSortParams(_).toOption).getOrElse(RawSortParam.empty)
        val withDescendants = queryString.keySet("with-descendants") || queryString.keySet("recursive")
        val withDeleted = queryString.keySet("with-deleted")
        val pathFilter = Some(PathFilter(normalizedPath, withDescendants))
        val withHistory = queryString.keySet("with-history")
        val debugInfo = queryString.keySet("debug-info")
        val gqp = queryString.keySet("gqp")
        val xg = queryString.keySet("xg")
        val yg = queryString.keySet("yg")

        if (offset > Settings.maxOffset) {
          Future.successful(BadRequest(s"Even Google doesn't handle offsets larger than ${Settings.maxOffset}!"))
        } else if (length > Settings.maxLength) {
          Future.successful(BadRequest(s"Length is larger than ${Settings.maxLength}!"))
        } else if (withHistory && (xg || yg || gqp))
          Future.successful(BadRequest(s"you can't mix `xg` nor `yg` with `with-history`: it makes no sense!"))
        else if (!withData && (xg || (yg && Get.getQueryString("yg").get.trim.startsWith(">")) || (gqp && Get.getQueryString(
          "gqp"
        ).get.trim.startsWith(">"))))
          Future.successful(
            BadRequest(
              s"you can't mix `xg` nor '>' prefixed `yg`/`gqp` expressions without also specifying `with-data`: it makes no sense!"
            )
          )
        else {
          val fieldSortParamsFut =
            RawSortParam.eval(rawSortParams, crudServiceFS, typesCache, cmwellRDFHelper, requestReceivedTimestamp)
          val fieldsFiltersFut = qpOpt.fold[Future[Option[FieldFilter]]](Future.successful(Option.empty[FieldFilter]))(
            rff => RawFieldFilter.eval(rff, typesCache, cmwellRDFHelper, requestReceivedTimestamp).map(Some.apply)
          )
          fieldsFiltersFut
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
              case Success(fieldFilters) =>
                fieldSortParamsFut.flatMap { fieldSortParams =>
                  crudServiceFS
                    .search(pathFilter,
                      fieldFilters,
                      Some(DatesFilter(from, to)),
                      PaginationParams(offset, length),
                      withHistory,
                      withData,
                      fieldSortParams,
                      debugInfo,
                      withDeleted)
                    .flatMap { unmodifiedSearchResult =>
                      val gqpModified = getQueryString("gqp").fold(Future.successful(unmodifiedSearchResult.infotons)) {
                        gqpPattern =>
                          gqpFilter(
                            gqpPattern,
                            unmodifiedSearchResult.infotons,
                            cmwellRDFHelper,
                            typesCache,
                            getQueryString("gqp-chunk-size").flatMap(asInt).getOrElse(10),
                            requestReceivedTimestamp
                          )
                      }

                      val ygModified = getQueryString("yg") match {
                        case Some(ygp) =>
                          gqpModified.flatMap { gqpFilteredInfotons =>
                            pathExpansionParser(ygp,
                              gqpFilteredInfotons,
                              getQueryString("yg-chunk-size").flatMap(asInt).getOrElse(10),
                              cmwellRDFHelper,
                              typesCache,
                              requestReceivedTimestamp).map {
                              case (ok, infotons) =>
                                ok -> unmodifiedSearchResult.copy(
                                  length = infotons.size,
                                  infotons = infotons
                                )
                            }
                          }
                        case None =>
                          gqpModified.map { gqpFilteredInfotons =>
                            if (!gqp) true -> unmodifiedSearchResult
                            else
                              true -> unmodifiedSearchResult.copy(
                                length = gqpFilteredInfotons.length,
                                infotons = gqpFilteredInfotons
                              )
                          }
                      }

                      val fSearchResult = ygModified.flatMap {
                        case (true, sr) =>
                          getQueryString("xg") match {
                            case None => Future.successful(true -> sr)
                            case Some(xgp) => {
                              deepExpandGraph(xgp, sr.infotons, cmwellRDFHelper, typesCache, requestReceivedTimestamp)
                                .map {
                                  case (ok, infotons) =>
                                    ok -> unmodifiedSearchResult.copy(
                                      length = infotons.size,
                                      infotons = infotons
                                    )
                                }
                            }
                          }
                        case (b, sr) => Future.successful(b -> sr)
                      }

                      fSearchResult.flatMap {
                        case (ok, searchResult) =>
                          extractFieldsMask(getQueryString("fields"),
                            typesCache,
                            cmwellRDFHelper,
                            requestReceivedTimestamp).map { fieldsMask =>
                            // Prepare pagination info
                            val searchUrl = cmWellBase + normalizedPath + "?op=search"
                            val format = getQueryString("format").fold("")("&format=".+)
                            val descendants = getQueryString("with-descendants").fold("")("&with-descendants=".+)
                            val recursive = getQueryString("recursive").fold("")("&recursive=".+)

                            val from = searchResult.fromDate.fold("")(
                              f => "&from=" + URLEncoder.encode(fullDateFormatter.print(f), "UTF-8")
                            )
                            val to = searchResult.toDate.fold("")(
                              t => "&to=" + URLEncoder.encode(fullDateFormatter.print(t), "UTF-8")
                            )

                            val qp = getQueryString("qp").fold("")("&qp=" + URLEncoder.encode(_, "UTF-8"))

                            val lengthParam = "&length=" + searchResult.length

                            val linkBase = searchUrl + format + descendants + recursive + from + to + qp + lengthParam
                            val self = linkBase + "&offset=" + searchResult.offset
                            val first = linkBase + "&offset=0"
                            val last = searchResult.length match {
                              case l if l > 0 =>
                                linkBase + "&offset=" + ((searchResult.total / searchResult.length) * searchResult.length)
                              case _ => linkBase + "&offset=0"
                            }

                            val next = searchResult.offset + searchResult.length - searchResult.total match {
                              case x if x < 0 =>
                                Some(linkBase + "&offset=" + (searchResult.offset + searchResult.length))
                              case _ => None
                            }

                            val previous = (searchResult.offset - searchResult.length) match {
                              case dif if dif >= 0                              => Some(linkBase + "&offset=" + dif)
                              case dif if dif < 0 && -dif < searchResult.length => Some(linkBase + "&offset=0")
                              case _                                            => None
                            }

                            val paginationInfo = PaginationInfo(first, previous, self, next, last)

                            //TODO: why not check for valid format before doing all the hard work for search?
                            getQueryString("format").getOrElse("atom") match {
                              case FormatExtractor(formatType) => {
                                val formatter = formatterManager.getFormatter(
                                  format = formatType,
                                  timeContext = requestReceivedTimestamp,
                                  host = requestHost,
                                  uri = requestUri,
                                  pretty = queryString.keySet("pretty"),
                                  callback = queryString.get("callback").flatMap(_.headOption),
                                  fieldFilters = fieldFilters,
                                  offset = Some(offset.toLong),
                                  length = Some(length.toLong),
                                  withData = withDataFormat,
                                  forceUniqueness = withHistory
                                )
                                if (ok)
                                  Ok(formatter.render(SearchResponse(paginationInfo, searchResult.masked(fieldsMask))))
                                    .as(OutputHandler.overrideMimetype(formatter.mimetype, Get.getQueryString("override-mimetype"))._2)
                                else
                                  InsufficientStorage(formatter.render(SearchResponse(paginationInfo, searchResult)))
                                    .as(OutputHandler.overrideMimetype(formatter.mimetype, Get.getQueryString("override-mimetype"))._2)
                              }
                              case unrecognized: String => BadRequest(s"unrecognized format requested: $unrecognized")
                            }
                          }
                      }
                    }
                }
            }
            .recover(errorHandler)
        }
      }
      .recover(asyncErrorHandler)
      .get
}
