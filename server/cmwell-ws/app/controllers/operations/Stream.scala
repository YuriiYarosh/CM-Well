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

import controllers.operations.Consumer

import actions.ActiveInfotonGenerator
import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cmwell.domain.IterationResults
import cmwell.formats.{FormatExtractor, NTriplesFlavor, NquadsFlavor, RdfType}
import cmwell.fts._
import cmwell.util.concurrent.SimpleScheduler
import cmwell.util.stream.StreamEventInspector
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.{Settings, Streams}
import cmwell.ws.adt.SortedConsumeState
import cmwell.ws.util.{DateParser, FieldFilterParser, FromDate, ToDate}
import cmwell.ws.util.TypeHelpers.{asInt, asLong}
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
import wsutil.{FormatterManager, RawFieldFilter, asyncErrorHandler, errorHandler, extractFieldsMask, normalizePath, overrideMimetype}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class Stream @Inject()(bulkScrollHandler: BulkScrollHandler,
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

  private def transformFieldFiltersForConsumption(fieldFilters: Option[FieldFilter],
                                                  timeStamp: Long,
                                                  now: Long): FieldFilter = {
    val fromOp =
      if (timeStamp != 0L) GreaterThan
      else GreaterThanOrEquals

    val fromFilter = FieldFilter(Must, fromOp, "system.indexTime", timeStamp.toString)
    val uptoFilter = FieldFilter(Must, LessThan, "system.indexTime", (now - 10000).toString)
    val rangeFilters = List(fromFilter, uptoFilter)

    fieldFilters match {
      case None => MultiFieldFilter(Must, rangeFilters)
      case Some(should@SingleFieldFilter(Should, _, _, _)) =>
        MultiFieldFilter(Must, should.copy(fieldOperator = Must) :: rangeFilters)
      case Some(ff) => MultiFieldFilter(Must, ff :: rangeFilters)
    }
  }

  private def handleStream(request: Request[AnyContent]): Future[Result] =
    request
      .getQueryString("qp")
      .fold(Success(None): Try[Option[RawFieldFilter]])(FieldFilterParser.parseQueryParams(_).map(Some.apply))
      .map { qpOpt =>
        val normalizedPath = normalizePath(request.path)
        val from = DateParser.parseDate(request.getQueryString("from").getOrElse(""), FromDate).toOption
        val to = DateParser.parseDate(request.getQueryString("to").getOrElse(""), ToDate).toOption
        val withDescendants = request.queryString.keySet("with-descendants") || request.queryString.keySet("recursive")
        val withHistory = request.queryString.keySet("with-history")
        val withDeleted = request.queryString.keySet("with-deleted")
        val withMeta = request.queryString.keySet("with-meta")
        val debugLog = request.queryString.keySet("debug-log")
        val scrollTtl = request.getQueryString("session-ttl").flatMap(asLong).getOrElse(3600L).min(3600L)
        val chunkSize = request.getQueryString("chunk-size").flatMap(asInt).getOrElse(500)
        val length = request.getQueryString("length").flatMap(asLong)
        val pathFilter = Some(PathFilter(normalizedPath, withDescendants))
        val timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp)
        val fieldsMaskFut = extractFieldsMask(request, typesCache, cmwellRDFHelper, timeContext)
        val (withData, format) = {
          val wd = request.getQueryString("with-data")
          val frmt = request
            .getQueryString("format")
            .getOrElse({
              if (wd.isEmpty) "text" else "nt"
            })
          if (Set("nt", "ntriples", "nq", "nquads").exists(frmt.equalsIgnoreCase) || frmt.toLowerCase.startsWith(
            "json"
          )) {
            Some("text") -> frmt
          } else {
            None -> frmt
          }
        }
        (format match {
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
            val fieldsFiltersFut = qpOpt.fold(Future.successful(Option.empty[FieldFilter]))(
              rff => RawFieldFilter.eval(rff, typesCache, cmwellRDFHelper, timeContext).map(Some.apply)
            )
            fieldsFiltersFut.transformWith {
              case Failure(err) => {
                val msg = s"failed to evaluate given qp [${qpOpt.fold("")(_.toString)}]"
                logger.error(msg, err)
                val res = FailedDependency(Json.obj("success" -> false, "message" -> msg))
                request.attrs.get(Attrs.RequestReceivedTimestamp).fold(Future.successful(res)) { reqStartTime =>
                  val timePassedInMillis = System.currentTimeMillis() - reqStartTime
                  if (timePassedInMillis > 9000L) Future.successful(res)
                  else SimpleScheduler.schedule((9500L - timePassedInMillis).millis)(res)
                }
              }
              case Success(fieldFilter) => {
                fieldsMaskFut.flatMap { fieldsMask =>
                  /* RDF types allowed in stream are: ntriples, nquads, jsonld & jsonldq
                   * since, the jsons are not realy RDF, just flattened json of infoton per line,
                   * there is no need to tnforce subject uniquness. but ntriples, and nquads
                   * which split infoton into statements (subject-predicate-object triples) per line,
                   * we don't want different versions to "mix" and we enforce uniquness only in this case
                   */
                  val forceUniqueness: Boolean = withHistory && (formatType match {
                    case RdfType(NquadsFlavor) => true
                    case RdfType(NTriplesFlavor) => true
                    case _ => false
                  })
                  val formatter = formatterManager.getFormatter(
                    format = formatType,
                    timeContext = timeContext,
                    host = request.host,
                    uri = request.uri,
                    pretty = false,
                    callback = request.queryString.get("callback").flatMap(_.headOption),
                    fieldFilters = fieldFilter,
                    withData = withData,
                    withoutMeta = !withMeta,
                    filterOutBlanks = true,
                    forceUniqueness = forceUniqueness
                  )

                  lazy val id = cmwell.util.numeric.Radix64.encodeUnsigned(request.id)

                  val debugLogID = if (debugLog) Some(id) else None

                  streams
                    .scrollSource(
                      pathFilter = pathFilter,
                      fieldFilters = fieldFilter,
                      datesFilter = Some(DatesFilter(from, to)),
                      paginationParams = PaginationParams(0, chunkSize),
                      scrollTTL = scrollTtl,
                      withHistory = withHistory,
                      withDeleted = withDeleted,
                      debugLogID = debugLogID
                    )
                    .map {
                      case (src, hits) =>
                        val s: Source[ByteString, NotUsed] = {
                          val srcWithDebug = if (debugLog) src.via {
                            new StreamEventInspector(
                              onUpstreamFinishInspection = () => logger.info(s"scrollSource<->scrollSourceToByteString [$id] onUpstreamFinish"),
                              onUpstreamFailureInspection = error => logger.error(s"scrollSource<->scrollSourceToByteString [$id] onUpstreamFailure", error),
                              onDownstreamFinishInspection = () => logger.info(s"scrollSource<->scrollSourceToByteString [$id] onDownstreamFinish"),
                              onPullInspection = () => logger.info(s"scrollSource<->scrollSourceToByteString [$id] onPull"),
                              onPushInspection = {
                                case IterationResults(_, totalHits, iSeqOpt, _, _) =>
                                  val infotonData = iSeqOpt.fold("infoton sequence is None")(iSeq =>
                                    s"infoton count: ${iSeq.size}, first uuid: ${iSeq.headOption.fold("none")(_.uuid)}")
                                  logger.info(s"scrollSource<->scrollSourceToByteString [$id] onPush(totalHits: $totalHits, $infotonData)")
                              }
                            )
                          } else src
                          val scrollSourceToByteString = streams.scrollSourceToByteString(srcWithDebug,
                            formatter,
                            withData.isDefined,
                            withHistory,
                            length,
                            fieldsMask)
                          if (debugLog) scrollSourceToByteString.via {
                            new StreamEventInspector(
                              onUpstreamFinishInspection = () => logger.info(s"scrollSourceToByteString<->Play [$id] onUpstreamFinish"),
                              onUpstreamFailureInspection = error => logger.error(s"scrollSourceToByteString<->Play [$id] onUpstreamFailure", error),
                              onDownstreamFinishInspection = () => logger.info(s"scrollSourceToByteString<->Play [$id] onDownstreamFinish"),
                              onPullInspection = () => logger.info(s"scrollSourceToByteString<->Play [$id] onPull"),
                              onPushInspection = bytes => {
                                val all = bytes.utf8String
                                val elem = {
                                  if (bytes.isEmpty) ""
                                  else all.lines.next()
                                }
                                logger.info(
                                  s"""scrollSourceToByteString<->Play [$id] onPush(first line: "$elem", num of lines: ${all.lines.size},"""
                                    + s" num of chars: ${all.length})")
                              }
                            )
                          } else scrollSourceToByteString
                        }
                        val headers = {
                          if (debugLog) List("X-CM-WELL-N" -> hits.toString, "X-CM-WELL-LOG-ID" -> id)
                          else List("X-CM-WELL-N" -> hits.toString)
                        }
                        Ok.chunked(s).as(overrideMimetype(formatter.mimetype, request)._2).withHeaders(headers: _*)
                    }
                }
              }
            }
          }
        }).recover(errorHandler)
      }
      .recover(asyncErrorHandler)
      .get

  /**
    * answers with a stream, which is basically a chunked response,
    * where each chunk is a reply from a scroll session, and there
    * are multiple scroll sessions (1 per index in ES).
    *
    * @param request
    * @return
    */
  private def handleBoostedStream(request: Request[AnyContent]): Future[Result] =
    request
      .getQueryString("qp")
      .fold(Success(None): Try[Option[RawFieldFilter]])(FieldFilterParser.parseQueryParams(_).map(Some.apply))
      .map { qpOpt =>
        val normalizedPath = normalizePath(request.path)
        val from = DateParser.parseDate(request.getQueryString("from").getOrElse(""), FromDate).toOption
        val to = DateParser.parseDate(request.getQueryString("to").getOrElse(""), ToDate).toOption
        val offset = 0 //request.getQueryString("offset").flatMap(asInt).getOrElse(0)
        val withDescendants = request.queryString.keySet("with-descendants") || request.queryString.keySet("recursive")
        val withHistory = request.queryString.keySet("with-history")
        val withDeleted = request.queryString.keySet("with-deleted")
        val withMeta = request.queryString.keySet("with-meta")
        val length = request.getQueryString("length").flatMap(asLong)
        val pathFilter = Some(PathFilter(normalizedPath, withDescendants))
        val timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp)
        val fieldsMaskFut = extractFieldsMask(request, typesCache, cmwellRDFHelper, timeContext)
        val (withData, format) = {
          val wd = request.getQueryString("with-data")
          val frmt = request
            .getQueryString("format")
            .getOrElse({
              if (wd.isEmpty) "text" else "nt"
            })
          if (Set("nt", "ntriples", "nq", "nquads").exists(frmt.equalsIgnoreCase) || frmt.toLowerCase.startsWith(
            "json"
          )) {
            Some("text") -> frmt
          } else {
            None -> frmt
          }
        }
        format match {
          case f
            if !Set("text", "path", "tsv", "tab", "nt", "ntriples", "nq", "nquads")(f.toLowerCase) && !f.toLowerCase
              .startsWith("json") =>
            Future.successful(
              BadRequest(
                Json.obj("success" -> false,
                  "message" -> "not a streamable type (use 'text','tsv','ntriples', 'nquads', or any json)")
              )
            )
          case FormatExtractor(formatType) => {
            val fieldsFiltersFut = qpOpt.fold(Future.successful(Option.empty[FieldFilter]))(
              rff => RawFieldFilter.eval(rff, typesCache, cmwellRDFHelper, timeContext).map(Some.apply)
            )
            fieldsFiltersFut
              .transformWith {
                case Failure(err) => {
                  val msg = s"failed to evaluate given qp [${qpOpt.fold("")(_.toString)}]"
                  logger.error(msg, err)
                  val res = FailedDependency(Json.obj("success" -> false, "message" -> msg))
                  request.attrs.get(Attrs.RequestReceivedTimestamp).fold(Future.successful(res)) { reqStartTime =>
                    val timePassedInMillis = System.currentTimeMillis() - reqStartTime
                    if (timePassedInMillis > 9000L) Future.successful(res)
                    else SimpleScheduler.schedule((9500L - timePassedInMillis).millis)(res)
                  }
                }
                case Success(fieldFilter) => {
                  fieldsMaskFut
                    .flatMap { fieldsMask =>
                      /* RDF types allowed in mstream are: ntriples, nquads, jsonld & jsonldq
                       * since, the jsons are not realy RDF, just flattened json of infoton per line,
                       * there is no need to tnforce subject uniquness. but ntriples, and nquads
                       * which split infoton into statements (subject-predicate-object triples) per line,
                       * we don't want different versions to "mix" and we enforce uniquness only in this case
                       */
                      val forceUniqueness: Boolean = withHistory && (formatType match {
                        case RdfType(NquadsFlavor) => true
                        case RdfType(NTriplesFlavor) => true
                        case _ => false
                      })
                      val formatter = formatterManager.getFormatter(
                        format = formatType,
                        timeContext = timeContext,
                        host = request.host,
                        uri = request.uri,
                        pretty = false,
                        callback = request.queryString.get("callback").flatMap(_.headOption),
                        fieldFilters = fieldFilter,
                        offset = Some(offset.toLong),
                        length = Some(500L),
                        withData = withData,
                        withoutMeta = !withMeta,
                        filterOutBlanks = true,
                        forceUniqueness = forceUniqueness
                      ) // cleanSystemBlanks set to true, so we won't output all the meta information we usaly output.
                      // it get's messy with streaming. we don't want each chunk to show the "document context"

                      val datesFilter = {
                        if (from.isEmpty && to.isEmpty) None
                        else Some(DatesFilter(from, to))
                      }
                      streams
                        .multiScrollSource(pathFilter = pathFilter,
                          fieldFilter = fieldFilter,
                          datesFilter = datesFilter,
                          withHistory = withHistory,
                          withDeleted = withDeleted)
                        .map {
                          case (source, hits) => {
                            val s = streams.scrollSourceToByteString(source,
                              formatter,
                              withData.isDefined,
                              withHistory,
                              length,
                              fieldsMask)
                            Ok.chunked(s)
                              .as(overrideMimetype(formatter.mimetype, request)._2)
                              .withHeaders("X-CM-WELL-N" -> hits.toString)
                          }
                        }
                        .recover(errorHandler)
                    }
                    .recover(errorHandler)
                }
              }
              .recover(errorHandler)
          }
        }
      }
      .recover(asyncErrorHandler)
      .get

  private def handleSuperStream(request: Request[AnyContent]): Future[Result] =
    request
      .getQueryString("qp")
      .fold(Success(None): Try[Option[RawFieldFilter]])(FieldFilterParser.parseQueryParams(_).map(Some.apply))
      .map { qpOpt =>
        val normalizedPath = normalizePath(request.path)
        val from = DateParser.parseDate(request.getQueryString("from").getOrElse(""), FromDate).toOption
        val to = DateParser.parseDate(request.getQueryString("to").getOrElse(""), ToDate).toOption
        val offset = 0
        //request.getQueryString("offset").flatMap(asInt).getOrElse(0)
        val withDescendants = request.queryString.keySet("with-descendants") || request.queryString.keySet("recursive")
        val withHistory = request.queryString.keySet("with-history")
        val withDeleted = request.queryString.keySet("with-deleted")
        val withMeta = request.queryString.keySet("with-meta")
        val length = request.getQueryString("length").flatMap(asLong)
        val parallelism = request.getQueryString("parallelism").flatMap(asInt).getOrElse(Settings.sstreamParallelism)
        val pathFilter = Some(PathFilter(normalizedPath, withDescendants))
        val timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp)
        val fieldsMaskFut = extractFieldsMask(request, typesCache, cmwellRDFHelper, timeContext)
        val (withData, format) = {
          val wd = request.getQueryString("with-data")
          val frmt = request
            .getQueryString("format")
            .getOrElse({
              if (wd.isEmpty) "text" else "nt"
            })
          if (Set("nt", "ntriples", "nq", "nquads").exists(frmt.equalsIgnoreCase) || frmt.toLowerCase.startsWith(
            "json"
          )) {
            Some("text") -> frmt
          } else {
            None -> frmt
          }
        }
        format match {
          case f
            if !Set("text", "path", "tsv", "tab", "nt", "ntriples", "nq", "nquads")(f.toLowerCase) && !f.toLowerCase
              .startsWith("json") =>
            Future.successful(
              BadRequest(
                Json.obj("success" -> false,
                  "message" -> "not a streamable type (use 'text','tsv','ntriples', 'nquads', or any json)")
              )
            )
          case FormatExtractor(formatType) => {
            val fieldsFiltersFut = qpOpt.fold(Future.successful(Option.empty[FieldFilter]))(
              rff => RawFieldFilter.eval(rff, typesCache, cmwellRDFHelper, timeContext).map(Some.apply)
            )
            fieldsFiltersFut.transformWith {
              case Failure(err) => {
                val msg = s"failed to evaluate given qp [${qpOpt.fold("")(_.toString)}]"
                logger.error(msg, err)
                val res = FailedDependency(Json.obj("success" -> false, "message" -> msg))
                request.attrs.get(Attrs.RequestReceivedTimestamp).fold(Future.successful(res)) { reqStartTime =>
                  val timePassedInMillis = System.currentTimeMillis() - reqStartTime
                  if (timePassedInMillis > 9000L) Future.successful(res)
                  else SimpleScheduler.schedule((9500L - timePassedInMillis).millis)(res)
                }
              }
              case Success(fieldFilter) => {
                fieldsMaskFut.flatMap { fieldsMask =>
                  /* RDF types allowed in mstream are: ntriples, nquads, jsonld & jsonldq
                   * since, the jsons are not realy RDF, just flattened json of infoton per line,
                   * there is no need to tnforce subject uniquness. but ntriples, and nquads
                   * which split infoton into statements (subject-predicate-object triples) per line,
                   * we don't want different versions to "mix" and we enforce uniquness only in this case
                   */
                  val forceUniqueness: Boolean = withHistory && (formatType match {
                    case RdfType(NquadsFlavor) => true
                    case RdfType(NTriplesFlavor) => true
                    case _ => false
                  })
                  val formatter = formatterManager.getFormatter(
                    format = formatType,
                    timeContext = timeContext,
                    host = request.host,
                    uri = request.uri,
                    pretty = false,
                    callback = request.queryString.get("callback").flatMap(_.headOption),
                    fieldFilters = fieldFilter,
                    offset = Some(offset.toLong),
                    length = Some(500L),
                    withData = withData,
                    withoutMeta = !withMeta,
                    filterOutBlanks = true,
                    forceUniqueness = forceUniqueness
                  ) // cleanSystemBlanks set to true, so we won't output all the meta information we usaly output.
                  // it get's messy with streaming. we don't want each chunk to show the "document context"

                  streams
                    .superScrollSource(
                      pathFilter = pathFilter,
                      fieldFilter = fieldFilter,
                      datesFilter = Some(DatesFilter(from, to)),
                      paginationParams = PaginationParams(offset, 500),
                      withHistory = withHistory,
                      withDeleted = withDeleted,
                      parallelism = parallelism
                    )
                    .map {
                      case (src, hits) =>
                        val s = streams.scrollSourceToByteString(src,
                          formatter,
                          withData.isDefined,
                          withHistory,
                          length,
                          fieldsMask)
                        Ok.chunked(s)
                          .as(overrideMimetype(formatter.mimetype, request)._2)
                          .withHeaders("X-CM-WELL-N" -> hits.toString)
                    }
                }
              }
            }
          }
        }
      }
      .recover(asyncErrorHandler)
      .get

  private def handleQueueStream(request: Request[AnyContent]): Future[Result] = {

    val indexTime: Long = request.getQueryString("index-time").flatMap(asLong).getOrElse(0L)
    val withMeta = request.queryString.keySet("with-meta")
    val length = request.getQueryString("length").flatMap(asLong)
    val lengthHint = request.getQueryString("length-hint").flatMap(asInt).getOrElse(3000)
    val normalizedPath = normalizePath(request.path)
    val qpOpt = request.getQueryString("qp")
    //deprecated!
    //    val from = request.getQueryString("from")
    //    val to = request.getQueryString("to")
    val withDescendants = request.queryString.keySet("with-descendants") || request.queryString.keySet("recursive")
    val withHistory = request.queryString.keySet("with-history")
    val withDeleted = request.queryString.keySet("with-deleted")
    val timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp)
    val (withData, format) = {
      val wd = request.getQueryString("with-data")
      val frmt = request
        .getQueryString("format")
        .getOrElse({
          if (wd.isEmpty) "text" else "nt"
        })
      if (Set("nt", "ntriples", "nq", "nquads").exists(frmt.equalsIgnoreCase) || frmt.toLowerCase.startsWith("json"))
        Some("text") -> frmt
      else None -> frmt
    }

    format match {
      case f
        if !Set("text", "path", "tsv", "tab", "nt", "ntriples", "nq", "nquads")(f.toLowerCase) && !f.toLowerCase
          .startsWith("json") =>
        Future.successful(
          BadRequest(
            Json
              .obj("success" -> false,
                "message" -> "not a streamable type (use any json, or one of: 'text','tsv','ntriples', or 'nquads')")
          )
        )
      case FormatExtractor(formatType) => {
        /* RDF types allowed in stream are: ntriples, nquads, jsonld & jsonldq
         * since, the jsons are not realy RDF, just flattened json of infoton per line,
         * there is no need to enforce subject uniquness. but ntriples, and nquads
         * which split infoton into statements (subject-predicate-object triples) per line,
         * we don't want different versions to "mix" and we enforce uniquness only in this case
         */
        val forceUniqueness: Boolean = withHistory && (formatType match {
          case RdfType(NquadsFlavor) => true
          case RdfType(NTriplesFlavor) => true
          case _ => false
        })

        Consumer.generateSortedConsumeFieldFilters(
          qpOpt = qpOpt,
          path = normalizedPath,
          withDescendants = withDescendants,
          withHistory = withHistory,
          withDeleted = withDeleted,
          indexTime = indexTime,
          timeContext = timeContext
        ).transformWith {
          case Failure(err) => {
            val msg = s"failed to evaluate given qp [${qpOpt.fold("")(_.toString)}]"
            logger.error(msg, err)
            val res = FailedDependency(Json.obj("success" -> false, "message" -> msg))
            request.attrs.get(Attrs.RequestReceivedTimestamp).fold(Future.successful(res)) { reqStartTime =>
              val timePassedInMillis = System.currentTimeMillis() - reqStartTime
              if (timePassedInMillis > 9000L) Future.successful(res)
              else SimpleScheduler.schedule((9500L - timePassedInMillis).millis)(res)
            }
          }
          case Success(SortedConsumeState(firstTimeStamp, path, history, deleted, descendants, fieldFilters)) => {

            val formatter = formatterManager.getFormatter(
              format = formatType,
              timeContext = timeContext,
              host = request.host,
              uri = request.uri,
              pretty = false,
              fieldFilters = fieldFilters,
              withData = withData,
              withoutMeta = !withMeta,
              filterOutBlanks = true,
              forceUniqueness = forceUniqueness
            )

            import cmwell.ws.Streams._

            val src = streams.qStream(firstTimeStamp, path, history, deleted, descendants, lengthHint, fieldFilters)

            val ss: Source[ByteString, NotUsed] = length.fold {
              if (withData.isEmpty)
                src.via(Flows.searchThinResultToByteString(formatter))
              else
                src
                  .via(Flows.searchThinResultToFatInfoton(crudServiceFS))
                  .via(Flows.infotonToByteString(formatter))
            } { l =>
              if (withData.isEmpty)
                src
                  .take(l)
                  .via(Flows.searchThinResultToByteString(formatter))
              else
                src
                  .via(Flows.searchThinResultToFatInfoton(crudServiceFS))
                  .take(l)
                  .via(Flows.infotonToByteString(formatter))
            }

            val contentType = {
              if (formatType.mimetype.startsWith("application/json"))
                overrideMimetype("application/json-seq;charset=UTF8", request)._2
              else
                overrideMimetype(formatType.mimetype, request)._2
            }

            Future.successful(Ok.chunked(ss.batch(128, identity)(_ ++ _)).as(contentType)) //TODO: `.withHeaders("X-CM-WELL-N" -> total.toString)`
          }
        }
          .recover(errorHandler)
      }
    }
  }
}
