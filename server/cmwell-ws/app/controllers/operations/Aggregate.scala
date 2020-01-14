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
import cmwell.formats.FormatExtractor
import cmwell.fts.{DatesFilter, FieldFilter, PaginationParams, PathFilter}
import cmwell.util.concurrent.SimpleScheduler
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.util.{AggregationsFiltersParser, DateParser, FieldFilterParser, FromDate, ToDate}
import cmwell.ws.util.TypeHelpers.asInt
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, BulkScrollHandler, CachedSpa, FileInfotonCaching}
import filters.Attrs
import javax.inject.Inject
import ld.cmw.passiveFieldTypesCacheImpl
import ld.exceptions.BadFieldTypeException
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.libs.json.Json
import play.api.mvc.{AnyContent, InjectedController, Request, Result}
import wsutil.{FormatterManager, RawAggregationFilter, RawFieldFilter, asyncErrorHandler, errorHandler, normalizePath, overrideMimetype}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class Aggregate @Inject()(crudServiceFS: CRUDServiceFS,
                          cmwellRDFHelper: CMWellRDFHelper,
                          formatterManager: FormatterManager,
                          assetsMetadataProvider: AssetsMetadataProvider,
                          assetsConfigurationProvider: AssetsConfigurationProvider,
                          servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {

  lazy val typesCache: passiveFieldTypesCacheImpl = crudServiceFS.passiveFieldTypesCache

  private[this] def translateAggregateException: PartialFunction[Throwable, Throwable] = {
    case e: org.elasticsearch.transport.RemoteTransportException
      if e.getCause.isInstanceOf[org.elasticsearch.action.search.SearchPhaseExecutionException]
        && e.getCause.getMessage.contains("cannot be cast to org.elasticsearch.index.fielddata.IndexNumericFieldData") =>
      new BadFieldTypeException(
        "Cannot cast field to numeric value. Did you try to use stats or histogram aggregations on non numeric field?", e)
    case e:IllegalArgumentException
      if e.getMessage.contains("aggregations failure due to text system field")
    => new IllegalArgumentException("Stats API is not supported for text system fields.", e)
    case e:IllegalArgumentException if e.getMessage.contains("aggregations failure due to fielddata disabled")
    => new IllegalArgumentException("Stats API does not support non-exact value operator for text fields. Please use :: instead of :", e)
    case e => e
  }

  private def handleAggregate(request: Request[AnyContent]): Future[Result] =
    request
      .getQueryString("qp")
      .fold(Success(None): Try[Option[RawFieldFilter]])(FieldFilterParser.parseQueryParams(_).map(Some.apply))
      .map { qpOpt =>
        val normalizedPath = normalizePath(request.path)
        val from = DateParser.parseDate(request.getQueryString("from").getOrElse(""), FromDate).toOption
        val to = DateParser.parseDate(request.getQueryString("to").getOrElse(""), ToDate).toOption
        val length = request.getQueryString("length").flatMap(asInt).getOrElse(10)
        val offset = request.getQueryString("offset").flatMap(asInt).getOrElse(0)
        val debugInfo = request.queryString.keySet("debug-info")
        val withDescendants = request.queryString.keySet("with-descendants") || request.queryString.keySet("recursive")
        val pathFilter = if (normalizedPath.length > 1) Some(PathFilter(normalizedPath, withDescendants)) else None
        val withHistory = request.queryString.keySet("with-history")
        val rawAggregationsFilters = AggregationsFiltersParser.parseAggregationParams(request.getQueryString("ap"))
        val timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp)

        rawAggregationsFilters match {
          case Success(raf) =>
            val apfut = Future.traverse(raf)(RawAggregationFilter.eval(_, typesCache, cmwellRDFHelper, timeContext))
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
                case Success(fieldFilters) => {
                  apfut.flatMap { af =>
                    crudServiceFS
                      .aggregate(pathFilter,
                        fieldFilters,
                        Some(DatesFilter(from, to)),
                        PaginationParams(offset, length),
                        withHistory,
                        af.flatten,
                        debugInfo)
                      .map { aggResult =>
                        request.getQueryString("format").getOrElse("json") match {
                          case FormatExtractor(formatType) => {
                            val formatter = formatterManager.getFormatter(
                              format = formatType,
                              timeContext = timeContext,
                              host = request.host,
                              uri = request.uri,
                              pretty = request.queryString.keySet("pretty"),
                              callback = request.queryString.get("callback").flatMap(_.headOption)
                            )
                            Ok(formatter.render(aggResult)).as(overrideMimetype(formatter.mimetype, request)._2)
                          }
                          case unrecognized: String => BadRequest(s"unrecognized format requested: $unrecognized")
                        }
                      }
                  }
                }
              }
              .recover(translateAggregateException.andThen(errorHandler))
          case Failure(e) => asyncErrorHandler(e)
        }
      }
      .recover(asyncErrorHandler)
      .get
}
