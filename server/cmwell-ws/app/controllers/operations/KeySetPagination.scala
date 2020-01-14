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
import akka.actor.ActorRef
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cmwell.domain.IterationResults
import cmwell.formats.{AtomFormatter, FormatExtractor}
import cmwell.fts.{DatesFilter, FieldFilter, PaginationParams, PathFilter}
import cmwell.util.concurrent.SimpleScheduler
import cmwell.util.string.Base64
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.util.{DateParser, FieldFilterParser, FromDate, ToDate}
import cmwell.ws.util.TypeHelpers.asInt
import com.google.common.cache.{Cache, CacheBuilder}
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, FileInfotonCaching, GetID, IterationState, IterationStateInput, IteratorIdDispatcher, ScrollInput, StartScrollInput}
import filters.Attrs
import javax.inject.Inject
import k.grid.Grid
import ld.cmw.passiveFieldTypesCacheImpl
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.libs.json.Json
import play.api.mvc.{Action, AnyContent, InjectedController, Request, Result}
import wsutil._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class KeySetPagination @Inject()(crudServiceFS: CRUDServiceFS,
                                 cmwellRDFHelper: CMWellRDFHelper,
                                 formatterManager: FormatterManager,
                                 assetsMetadataProvider: AssetsMetadataProvider,
                                 assetsConfigurationProvider: AssetsConfigurationProvider,
                                 servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {

  lazy val typesCache: passiveFieldTypesCacheImpl = crudServiceFS.passiveFieldTypesCache

  private def createScrollIdDispatcherActorFromIteratorId(iterationStateInput: IterationStateInput,
                                                          withHistory: Boolean,
                                                          ttl: FiniteDuration): String = {
    val ar = Grid.createAnon(classOf[IteratorIdDispatcher], iterationStateInput, withHistory, ttl)
    val path = ar.path.toSerializationFormatWithAddress(Grid.me)
    putInCache(path, ar)
    val rv = Base64.encodeBase64URLSafeString(path)
    logger.debug(s"created actor with id = $rv")
    rv
  }

  val (actorRefsCachedFactory, putInCache) = {
    val c: Cache[String, Future[Option[ActorRef]]] = CacheBuilder.newBuilder().maximumSize(100).build()
    val func: String => Future[Option[ActorRef]] = s => {
      Grid.getRefFromSelection(Grid.selectByPath(s), 10, 1.second).transform {
        case Failure(_: NoSuchElementException) => Success(None)
        case otherTry => otherTry.map(Some.apply)
      }
    }

    val putRefreshedValueEagerlyInCache: (String, ActorRef) => Unit =
      (k: String, v: ActorRef) => {
        c.put(k, Future.successful(Some(v)))
      }

    (cmwell.zcache.L1Cache
      .memoizeWithCache[String, Option[ActorRef]](func)(identity)(c)(ec)
      .andThen(_.transform {
        case Success(None) =>
          Failure(
            new NoSuchElementException("actor address was not found please see previous log for the exact reason why.")
          )
        case anotherNonEmptyTry => anotherNonEmptyTry.map(_.get)
      })) -> putRefreshedValueEagerlyInCache
  }

  private def ftsScroll(scrollInput: IterationStateInput, scrollTTL: Long, withData: Boolean, debugInfo:Boolean): Future[IterationResults] = {
    scrollInput match {
      case ScrollInput(scrollId) => crudServiceFS.scroll(scrollId, scrollTTL, withData, debugInfo)
      case StartScrollInput(pathFilter, fieldFilters, datesFilter, paginationParams, scrollTtl, withHistory, withDeleted, withData) =>
        crudServiceFS.startScroll(
          pathFilter,
          fieldFilters,
          datesFilter,
          paginationParams,
          scrollTtl,
          withHistory,
          withDeleted,
          debugInfo,
          withData)
    }
  }

  // This method is to enable to access handleScroll from routes.
  def handleScrollRoute = Action.async { implicit request =>
    handleScroll(request)
  }

  private def handleStartScroll(request: Request[AnyContent]): Future[Result] =
    request
      .getQueryString("qp")
      .fold(Success(None): Try[Option[RawFieldFilter]])(FieldFilterParser.parseQueryParams(_).map(Some.apply))
      .map { qpOpt =>
        val timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp)
        val normalizedPath = normalizePath(request.path)
        val from = DateParser.parseDate(request.getQueryString("from").getOrElse(""), FromDate).toOption
        val to = DateParser.parseDate(request.getQueryString("to").getOrElse(""), ToDate).toOption
        val length = request.getQueryString("length").flatMap(asInt).getOrElse(500)
        val offset = request.getQueryString("offset").flatMap(asInt).getOrElse(0)
        val scrollTtl = request.getQueryString("session-ttl").flatMap(asInt).getOrElse(15).min(60)
        val withDescendants = request.queryString.keySet("with-descendants") || request.queryString.keySet("recursive")
        val withHistory = request.queryString.keySet("with-history")
        val withDeleted = request.queryString.keySet("with-deleted")
        val pathFilter = Some(PathFilter(normalizedPath, withDescendants))
        val withData = request.getQueryString("with-data")
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
              val formatter = request.getQueryString("format").getOrElse("json") match {
                case FormatExtractor(formatType) =>
                  formatterManager.getFormatter(
                    format = formatType,
                    timeContext = timeContext,
                    host = request.host,
                    uri = request.uri,
                    pretty = request.queryString.keySet("pretty"),
                    callback = request.queryString.get("callback").flatMap(_.headOption),
                    fieldFilters = fieldFilter,
                    offset = Some(offset.toLong),
                    length = Some(length.toLong),
                    withData = withData
                  )
              }

              val fmFut = extractFieldsMask(request, typesCache, cmwellRDFHelper, timeContext)
              val debugInfoParam = request.queryString.keySet("debug-info")
              crudServiceFS
                .thinSearch(
                  pathFilter,
                  fieldFilter,
                  Some(DatesFilter(from, to)),
                  PaginationParams(offset, 1),
                  withHistory,
                  debugInfo = debugInfoParam,
                  withDeleted = withDeleted
                ).map { ftsResults =>
                IterationResults("", ftsResults.total, Some(Vector.empty), debugInfo = ftsResults.debugInfo)
              }.flatMap { thinSearchResult =>
                val withDataB = withData.fold(false)(_.toLowerCase() != "false")
                val rv = createScrollIdDispatcherActorFromIteratorId(StartScrollInput(pathFilter,
                  fieldFilter,
                  Some(DatesFilter(from, to)),
                  PaginationParams(offset, length),
                  scrollTtl,
                  withHistory,
                  withDeleted,
                  withDataB), withHistory, (scrollTtl + 5).seconds)
                fmFut.map { fm =>
                  Ok(formatter.render(thinSearchResult.copy(iteratorId = rv).masked(fm))).as(formatter.mimetype)
                }
              }
            }
          }
          .recover(errorHandler)
      }
      .recover(asyncErrorHandler)
      .get


  /**
    * WARNING: using xg with iterator, is at the user own risk!
    * results may be cut off if expansion limit is exceeded,
    * but no warning can be emitted, since we use a chunked response,
    * and it is impossible to change status code or headers.
    *
    * @param request
    * @return
    */
  private def handleScroll(request: Request[AnyContent]): Future[Result] =
    Try {

      import akka.pattern.ask

      request
        .getQueryString("iterator-id")
        .fold(Future.successful(BadRequest("iterator-id query param is mandatory for this operation"))) {
          encodedActorAddress =>
            val xg = request.getQueryString("xg")
            val (yg, ygChunkSize) = request
              .getQueryString("yg")
              .fold(Option.empty[String] -> 0)(
                Some(_) -> request.getQueryString("yg-chunk-size").flatMap(asInt).getOrElse(10)
              )
            val scrollTtl = request.getQueryString("session-ttl").flatMap(asInt).getOrElse(15).min(60)
            val withDataFormat = request.getQueryString("with-data")
            val timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp)
            val withData =
              (withDataFormat.isDefined && withDataFormat.get.toLowerCase != "false") ||
                (withDataFormat.isEmpty && (yg.isDefined || xg.isDefined)) //infer `with-data` implicitly, and don't fail the request

            val fieldsMaskFut = extractFieldsMask(request, typesCache, cmwellRDFHelper, timeContext)

            logger.debug(s"Get chunk request received with: xg=$xg yg=$yg scrollTtl=$scrollTtl withData=$withData")

            if (!withData && xg.isDefined)
              Future.successful(BadRequest("you can't use `xg` without also specifying `with-data`!"))
            else {
              val timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp)
              val deadLine = request.attrs(Attrs.RequestReceivedTimestamp) + 7000
              val itStateEitherFuture: Future[Either[String, IterationState]] = {
                actorRefsCachedFactory(Base64.decodeBase64String(encodedActorAddress, "UTF-8")).flatMap { ar =>
                  (ar ? GetID) (akka.util.Timeout(10.seconds)).mapTo[IterationState].map {
                    case IterationState(_, wh, _) if wh && (yg.isDefined || xg.isDefined) => {
                      Left(
                        "iterator is defined to contain history. you can't use `xg` or `yg` operations on histories."
                      )
                    }
                    case msg => Right(msg)
                  }
                }
              }
              request.getQueryString("format").getOrElse("atom") match {
                case FormatExtractor(formatType) =>
                  itStateEitherFuture
                    .flatMap {
                      case Left(errMsg) => Future.successful(BadRequest(errMsg))
                      case Right(IterationState(scrollInput, withHistory, ar)) => {
                        val formatter = formatterManager.getFormatter(
                          format = formatType,
                          timeContext = timeContext,
                          host = request.host,
                          uri = request.uri,
                          pretty = request.queryString.keySet("pretty"),
                          callback = request.queryString.get("callback").flatMap(_.headOption),
                          withData = withDataFormat,
                          forceUniqueness = withHistory
                        )
                        val debugInfo = request.queryString.keySet("debug-info")
                        val scrollIterationResults = ftsScroll(scrollInput, scrollTtl + 5, withData, debugInfo)
                        val futureThatMayHang: Future[String] =
                          scrollIterationResults.flatMap { tmpIterationResults =>
                            fieldsMaskFut.flatMap { fieldsMask =>
                              val rv = createScrollIdDispatcherActorFromIteratorId(ScrollInput(tmpIterationResults.iteratorId),
                                withHistory,
                                scrollTtl.seconds)
                              val iterationResults = tmpIterationResults.copy(iteratorId = rv).masked(fieldsMask)
                              val ygModified = yg match {
                                case Some(ygp) if iterationResults.infotons.isDefined => {
                                  pathExpansionParser(ygp,
                                    iterationResults.infotons.get,
                                    ygChunkSize,
                                    cmwellRDFHelper,
                                    typesCache,
                                    timeContext).map {
                                    case (ok, infotons) => ok -> iterationResults.copy(infotons = Some(infotons))
                                  }
                                }
                                case _ => Future.successful(true -> iterationResults)
                              }

                              ygModified.flatMap {
                                case (ok, iterationResultsAfterYg) => {
                                  (xg, iterationResultsAfterYg.infotons) match {
                                    case t if t._1.isEmpty || t._2.isEmpty || !ok =>
                                      Future(formatter.render(iterationResultsAfterYg))
                                    case (Some(xgp), Some(infotons)) => {
                                      val fIterationResults =
                                        deepExpandGraph(xgp, infotons, cmwellRDFHelper, typesCache, timeContext).map {
                                          case (_, iseq) => iterationResultsAfterYg.copy(infotons = Some(iseq))
                                        }
                                      fIterationResults.map(formatter.render)
                                    }
                                  }
                                }
                              }
                            }
                          }

                        // initialGraceTime can become negative
                        // (think of a lengthy GC that will cause now to be after the dead line),
                        // but it's ok because the scheduler will start immidiately
                        val initialGraceTime = {
                          val now = System.currentTimeMillis()
                          if (now >= deadLine) Duration.Zero
                          else Duration(deadLine - now, MILLISECONDS)
                        }
                        val injectInterval = 3.seconds
                        val backOnTime: String => Result = { str =>
                          Ok(str).as(overrideMimetype(formatter.mimetype, request)._2)
                        }
                        val prependInjections: () => ByteString = formatter match {
                          case a: AtomFormatter => {
                            val it = Iterator.single(ByteString(a.xsltRef)) ++ Iterator.continually(
                              cmwell.ws.Streams.endln
                            )
                            () =>
                              it.next()
                          }
                          case _ =>
                            () =>
                              cmwell.ws.Streams.endln
                        }
                        val injectOriginalFutureWith: String => ByteString = ByteString(_, StandardCharsets.UTF_8)
                        val continueWithSource: Source[ByteString, NotUsed] => Result = { src =>
                          Ok.chunked(src).as(overrideMimetype(formatter.mimetype, request)._2)
                        }

                        guardHangingFutureByExpandingToSource[String, ByteString, Result](
                          futureThatMayHang,
                          initialGraceTime,
                          injectInterval
                        )(backOnTime, prependInjections, injectOriginalFutureWith, continueWithSource)
                      }
                    }
                    .recover {
                      case err: Throwable => {
                        val actorAddress = Base64.decodeBase64String(encodedActorAddress, "UTF-8")
                        val id = actorAddress.split('/').last
                        logger.error(s"[ID: $id] actor holding actual ES ID could not be found ($actorAddress)", err)
                        ExpectationFailed(
                          "it seems like the iterator-id provided is invalid. " +
                            "either it was already retrieved, or the specified session-ttl timeout exceeded, " +
                            s"or it was malformed. error has been logged with ID = $id"
                        )
                      }
                    }
                case unrecognized: String =>
                  Future.successful(BadRequest(s"unrecognized format requested: $unrecognized"))
              }
            }
        }
    }.recover(asyncErrorHandler).get

}
