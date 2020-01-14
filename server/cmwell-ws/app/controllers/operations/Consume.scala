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

import akka.stream.scaladsl.Source
import cmwell.domain.{SearchThinResult, SearchThinResults}
import cmwell.formats.{FormatExtractor, Formatter}
import cmwell.fts.{Equals, FieldFilter, FieldSortParams, MultiFieldFilter, Must, PaginationParams, PathFilter}
import cmwell.syntaxutils.!!!
import cmwell.util.{BoxedFailure, EmptyBox, FullBox}
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.{Settings, Streams}
import cmwell.ws.adt.{BulkConsumeState, ConsumeState, SortedConsumeState}
import cmwell.ws.util.FieldFilterParser
import cmwell.ws.util.TypeHelpers.asInt
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, FileInfotonCaching}
import filters.Attrs
import javax.inject.Inject
import ld.cmw.passiveFieldTypesCacheImpl
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.mvc.{AnyContent, InjectedController, Request, Result}
import wsutil.{FormatterManager, RawFieldFilter, asyncErrorHandler, deepExpandGraph, errorHandler, extractFieldsMask, extractInferredFormatWithData, gqpFilter, overrideMimetype, pathExpansionParser}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class Consume @Inject()(crudServiceFS: CRUDServiceFS,
                        cmwellRDFHelper: CMWellRDFHelper,
                        formatterManager: FormatterManager,
                        assetsMetadataProvider: AssetsMetadataProvider,
                        assetsConfigurationProvider: AssetsConfigurationProvider,
                        servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {


  def expandSearchResultsForSortedIteration(newResults: immutable.Seq[SearchThinResult],
                                            sortedIteratorState: SortedConsumeState,
                                            total: Long,
                                            formatter: Formatter,
                                            contentType: String,
                                            xg: Option[String],
                                            yg: Option[String],
                                            gqp: Option[String],
                                            ygChunkSize: Int,
                                            gqpChunkSize: Int,
                                            timeContext: Option[Long],
                                            fieldsMask: Set[String]): Future[Result] = {

    lazy val typesCache: passiveFieldTypesCacheImpl = crudServiceFS.passiveFieldTypesCache

    val id = ConsumeState.encode(sortedIteratorState)

    if (formatter.format.isThin) {
      require(
        xg.isEmpty && yg.isEmpty,
        "Thin formats does not carry data, and thus cannot be expanded! (xg/yg/gqp supplied together with a thin format)"
      )
      val body = FormatterManager.formatFormattableSeq(newResults, formatter)
      Future.successful(
        Ok(body)
          .as(contentType)
          .withHeaders("X-CM-WELL-POSITION" -> id, "X-CM-WELL-N-LEFT" -> (total - newResults.length).toString)
      )
    } else if (xg.isEmpty && yg.isEmpty && gqp.isEmpty)
      Future.successful(
        Ok.chunked(
          Source(newResults)
            .via(Streams.Flows.searchThinResultToFatInfoton(crudServiceFS))
            .map(_.masked(fieldsMask))
            .via(Streams.Flows.infotonToByteString(formatter))
        )
          .as(contentType)
          .withHeaders("X-CM-WELL-POSITION" -> id, "X-CM-WELL-N-LEFT" -> (total - newResults.length).toString)
      )
    else {

      import cmwell.util.concurrent.travector

      travector(newResults)(str => crudServiceFS.getInfotonByUuidAsync(str.uuid).map(_ -> str.uuid)).flatMap {
        newInfotonsBoxes =>
          val newInfotons = newInfotonsBoxes.collect { case (FullBox(i), _) => i }

          if (newInfotons.length != newInfotonsBoxes.length) {
            val (fails, nones) = cmwell.util.collections.partitionWith(newInfotonsBoxes.filter(_._1.isEmpty)) {
              case (BoxedFailure(e), u) => Left(e -> u)
              case (EmptyBox, u) => Right(u)
              case _ => !!!
            }
            if (nones.nonEmpty) logger.error("some uuids could not be retrieved: " + nones.mkString("[", ",", "]"))
            fails.foreach {
              case (e, u) => logger.error(s"uuid [$u] failed", e)
            }
          }

          val gqpModified = gqp.fold(Future.successful(newInfotons))(
            gqpFilter(_, newInfotons, cmwellRDFHelper, typesCache, gqpChunkSize, timeContext).map(_.toVector)
          )
          gqpModified.flatMap { infotonsAfterGQP =>
            //TODO: xg/yg handling should be factor out (DRY principle)
            val ygModified = yg match {
              case Some(ygp) if infotonsAfterGQP.nonEmpty => {
                pathExpansionParser(ygp, infotonsAfterGQP, ygChunkSize, cmwellRDFHelper, typesCache, timeContext).map {
                  case (ok, infotonsAfterYg) => ok -> infotonsAfterYg
                }
              }
              case _ => Future.successful(true -> infotonsAfterGQP)
            }

            val maskedYgModified = ygModified.map { case (ok, infotons) => ok -> infotons.map(_.masked(fieldsMask)) }

            maskedYgModified.flatMap {
              case (false, infotonsAfterYg) => {
                val body = FormatterManager.formatFormattableSeq(infotonsAfterYg, formatter)
                val result = InsufficientStorage(body)
                  .as(contentType)
                  .withHeaders("X-CM-WELL-POSITION" -> id,
                    "X-CM-WELL-N-LEFT" -> (total - infotonsAfterGQP.length).toString)
                Future.successful(result)
              }
              case (true, infotonsAfterYg) if infotonsAfterYg.isEmpty || xg.isEmpty => {
                val body = FormatterManager.formatFormattableSeq(infotonsAfterYg, formatter)
                val result = {
                  if (newInfotonsBoxes.exists(_._1.isEmpty)) PartialContent(body)
                  else Ok(body)
                }

                Future.successful(
                  result
                    .as(contentType)
                    .withHeaders("X-CM-WELL-POSITION" -> id,
                      "X-CM-WELL-N-LEFT" -> (total - newInfotonsBoxes.length).toString)
                )
              }
              case (true, infotonsAfterYg) => {
                deepExpandGraph(xg.get, infotonsAfterYg, cmwellRDFHelper, typesCache, timeContext).map {
                  case (_, infotonsAfterXg) =>
                    val body = FormatterManager.formatFormattableSeq(infotonsAfterXg, formatter)
                    val result = {
                      if (newInfotonsBoxes.exists(_._1.isEmpty)) PartialContent(body)
                      else Ok(body)
                    }

                    result
                      .as(contentType)
                      .withHeaders("X-CM-WELL-POSITION" -> id,
                        "X-CM-WELL-N-LEFT" -> (total - newInfotonsBoxes.length).toString)
                }
              }
            }
          }
      }
    }
  }

  // This method is to enable to access handleConsume from routes.
  def handleConsumeRoute = Action.async { implicit request =>
    if (request.queryString.isEmpty) Future.successful(Ok(views.txt._consume(request)))
    else handleConsume(request)
  }

  // This method is to enable to access BulkScrollHandler.handle from routes.
  def handleBulkConsumeRoute = Action.async { implicit request =>
    if (request.queryString.isEmpty) Future.successful(Ok(views.txt._bulkConsume(request)))
    else bulkScrollHandler.handle(request)
  }

  private[controllers] def handleConsume(request: Request[AnyContent]): Future[Result] =
    Try {
      val sortedIteratorID = request
        .getQueryString("position")
        .getOrElse(throw new IllegalArgumentException("`position` parameter is required"))

      //properties that needs to be re-sent every iteration
      val xg = request.getQueryString("xg")
      val (yg, ygChunkSize) = request
        .getQueryString("yg")
        .fold(Option.empty[String] -> 0)(
          Some(_) -> request.getQueryString("yg-chunk-size").flatMap(asInt).getOrElse(10)
        )
      val (gqp, gqpChunkSize) = request
        .getQueryString("gqp")
        .fold(Option.empty[String] -> 0)(
          Some(_) -> request.getQueryString("gqp-chunk-size").flatMap(asInt).getOrElse(10)
        )
      val (requestedFormat, withData) = {
        val (f, b) = extractInferredFormatWithData(request, "json")
        f -> (b || yg.isDefined || xg.isDefined) //infer `with-data` implicitly, and don't fail the request
      }
      val isSimpleConsume = !(xg.isDefined || yg.isDefined || gqp.isDefined)

      def wasSupplied(queryParamKey: String) = request.queryString.keySet(queryParamKey)
      // scalastyle:off
      if (wasSupplied("qp"))
        Future.successful(BadRequest("you can't specify `qp` together with `position` (`qp` is meant to be used only in the first iteration request. after that, continue iterating using the received `position`)"))
      else if (wasSupplied("from") || wasSupplied("to"))
        Future.successful(BadRequest("`from`/`to` is determined in the beginning of the iteration. can't specify together with `position`"))
      else if (wasSupplied("indexTime"))
        Future.successful(BadRequest("`indexTime` is determined in the beginning of the iteration. can't specify together with `position`"))
      else if (wasSupplied("with-descendants") || wasSupplied("recursive"))
        Future.successful(BadRequest("`with-descendants`/`recursive` is determined in the beginning of the iteration. can't specify together with `position`"))
      else if (wasSupplied("with-history"))
        Future.successful(BadRequest("`with-history` is determined in the beginning of the iteration. can't specify together with `position`"))
      else if (wasSupplied("with-deleted"))
        Future.successful(BadRequest("`with-deleted` is determined in the beginning of the iteration. can't specify together with `position`"))
      // scalastyle:on
      else {
        val timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp)
        val sortedIteratorStateTry = ConsumeState.decode[SortedConsumeState](sortedIteratorID)
        val hardLimit = if (isSimpleConsume) Settings.consumeSimpleChunkSize else Settings.consumeExpandableChunkSize
        val lengthHint = request
          .getQueryString("length-hint")
          .flatMap(asInt)
          .getOrElse(
            ConsumeState
              .decode[BulkConsumeState](sortedIteratorID)
              .toOption
              .collect {
                case b if b.threshold <= Settings.maxLength => b.threshold.toInt
              }
              .getOrElse(hardLimit)
          ).min(hardLimit) //Making sure length hint is not bigger than max

        val debugInfo = request.queryString.keySet("debug-info")

        sortedIteratorStateTry
          .map {
            case sortedConsumeState@SortedConsumeState(timeStamp,
            path,
            history,
            deleted,
            descendants,
            fieldFilters) => {

              val pf = path.map(PathFilter(_, descendants))
              val ffs = transformFieldFiltersForConsumption(fieldFilters,
                timeStamp,
                request.attrs(Attrs.RequestReceivedTimestamp))
              val pp = PaginationParams(0, lengthHint)
              val fsp = FieldSortParams(List("system.indexTime" -> Asc))

              val future = crudServiceFS.thinSearch(
                pathFilter = pf,
                fieldFilters = Some(ffs),
                datesFilter = None,
                paginationParams = pp,
                withHistory = history,
                withDeleted = deleted,
                fieldSortParams = fsp,
                debugInfo = debugInfo
              )

              val (contentType, formatter) = requestedFormat match {
                case FormatExtractor(formatType) =>
                  val f = formatterManager.getFormatter(
                    format = formatType,
                    timeContext = timeContext,
                    host = request.host,
                    uri = request.uri,
                    pretty = request.queryString.keySet("pretty"),
                    callback = request.queryString.get("callback").flatMap(_.headOption),
                    fieldFilters = fieldFilters
                  )

                  val m =
                    if (formatType.mimetype.startsWith("application/json"))
                      overrideMimetype("application/json-seq;charset=UTF8", request)._2
                    else
                      overrideMimetype(formatType.mimetype, request)._2

                  (m, f)
              }

              future
                .flatMap {
                  case sr: SearchThinResults if sr.thinResults.isEmpty => {
                    if (debugInfo) {
                      logger.info(
                        s"""will emit 204 for search params:
                           |pathFilter       = $pf,
                           |fieldFilters     = $ffs,
                           |paginationParams = $pp,
                           |withHistory      = $history,
                           |fieldSortParams  = $fsp""".stripMargin)
                    }
                    val result = new Status(204)
                      .as(contentType)
                      .withHeaders("X-CM-WELL-POSITION" -> sortedIteratorID, "X-CM-WELL-N-LEFT" -> "0")
                    Future.successful(result)
                  }
                  case SearchThinResults(total, _, _, results, _) if results.nonEmpty => {

                    val idxT = results.maxBy(_.indexTime).indexTime //infotons.maxBy(_.indexTime.getOrElse(0L)).indexTime.getOrElse(0L)

                    val fieldsMaskFut = extractFieldsMask(request.getQueryString("fields"),
                      typesCache,
                      cmwellRDFHelper,
                      request.attrs.get(Attrs.RequestReceivedTimestamp))

                    // last chunk
                    if (results.length >= total)
                      fieldsMaskFut.flatMap { fieldsMask =>
                        expandSearchResultsForSortedIteration(results,
                          sortedConsumeState.copy(from = idxT),
                          total,
                          formatter,
                          contentType,
                          xg,
                          yg,
                          gqp,
                          ygChunkSize,
                          gqpChunkSize,
                          timeContext,
                          fieldsMask)
                      }
                    //regular chunk with more than 1 indexTime
                    else if (results.exists(_.indexTime != idxT)) {
                      fieldsMaskFut.flatMap { fieldsMask =>
                        val newResults = results.filter(_.indexTime < idxT)
                        val id = sortedConsumeState.copy(from = idxT - 1)
                        //expand the infotons with yg/xg, but only after filtering out the infotons with the max indexTime
                        expandSearchResultsForSortedIteration(newResults,
                          id,
                          total,
                          formatter,
                          contentType,
                          xg,
                          yg,
                          gqp,
                          ygChunkSize,
                          gqpChunkSize,
                          timeContext,
                          fieldsMask)
                      }
                    }
                    //all the infotons in current chunk have the same indexTime
                    else {

                      val ffs2 = {
                        val eqff = FieldFilter(Must, Equals, "system.indexTime", idxT.toString)
                        fieldFilters.fold[FieldFilter](eqff) { ff =>
                          MultiFieldFilter(Must, List(ff, eqff))
                        }
                      }

                      val scrollFuture = streams.scrollSource(pathFilter = pf,
                        fieldFilters = Some(ffs2),
                        withHistory = history,
                        withDeleted = deleted)

                      scrollFuture
                        .flatMap {
                          //if by pure luck, the chunk length is exactly equal to the number of infotons in cm-well containing this same indexTime
                          case (_, hits) if hits <= results.size =>
                            fieldsMaskFut.flatMap { fieldsMask =>
                              expandSearchResultsForSortedIteration(results,
                                sortedConsumeState.copy(from = idxT),
                                total,
                                formatter,
                                contentType,
                                xg,
                                yg,
                                gqp,
                                ygChunkSize,
                                gqpChunkSize,
                                timeContext,
                                fieldsMask)
                            }
                          // if we were asked to expand chunk, but need to respond with a chunked response
                          // (workaround: try increasing length or search directly with adding `system.indexTime::${idxT}`)
                          case _ if xg.isDefined || yg.isDefined =>
                            Future.successful(
                              UnprocessableEntity(
                                s"encountered a large chunk which cannot be expanded using xg/yg. (indexTime=$idxT)"
                              )
                            )
                          //chunked response
                          case (iterationResultsEnum, hits) => {
                            logger.info(s"sorted iteration encountered a large chunk [indexTime = $idxT]")

                            val id = ConsumeState.encode(sortedConsumeState.copy(from = idxT))

                            fieldsMaskFut.map { fieldsMask =>
                              val src =
                                streams.scrollSourceToByteString(iterationResultsEnum,
                                  formatter,
                                  withData,
                                  history,
                                  None,
                                  fieldsMask)

                              Ok
                                .chunked(src)
                                .as(contentType)
                                .withHeaders("X-CM-WELL-POSITION" -> id, "X-CM-WELL-N-LEFT" -> (total - hits).toString)
                            }
                          }
                        }
                        .recover(errorHandler)
                    }
                  }
                }
                .recover(errorHandler)
            }
          }
          .recover(asyncErrorHandler)
          .get
      }
    }.recover(asyncErrorHandler).get
}
