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
import cmwell.formats.{FormatExtractor, FormatType}
import cmwell.rts.{Pull, Push, Subscriber}
import cmwell.util.http.SimpleHttpClient
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.Streams
import cmwell.ws.util.RTSQueryPredicate
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, BulkScrollHandler, CachedSpa, FileInfotonCaching}
import filters.Attrs
import javax.inject.Inject
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.mvc.{AnyContent, InjectedController, Request, Result}
import security.AuthUtils
import wsutil.{FormatterManager, asyncErrorHandler, normalizePath}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class Subscription @Inject()(bulkScrollHandler: BulkScrollHandler,
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

  /*
    for subscribe

    pull
    http://[host:port]/[path]?op=subscribe&method=pull&qp=?

    push
    http://[host:port]/[path]?op=subscribe&method=push&callback=http://[host]/&qp=?

op=< subscribe | unsubscribe >
format=< json | yaml | n3 | ttl | rdfxml | ... >
method=< push | pull >
bulk-size=< [#infotons] >
subscription-key=< [KEY] >
qp=< [query predicate] >
callback=< [URL] >

   */

  def handleSubscribe(request: Request[AnyContent]): Future[Result] =
    Try {

      val timeContext = request.attrs.get(Attrs.RequestReceivedTimestamp)
      val path = normalizePath(request.path)
      val name = cmwell.util.os.Props.machineName
      val sub = cmwell.util.string.Hash.crc32(s"$timeContext#$name")
      //    val bulkSize = request.getQueryString("bulk-size").flatMap(asInt).getOrElse(50)
      request
        .getQueryString("qp")
        .map(RTSQueryPredicate.parseRule(_, path))
        .getOrElse(Right(cmwell.rts.PathFilter(new cmwell.rts.Path(path, true)))) match {
        case Left(error) => Future.successful(BadRequest(s"bad syntax for qp: $error"))
        case Right(rule) =>
          request.getQueryString("format").getOrElse("json") match {
            case FormatExtractor(format) =>
              request.getQueryString("method") match {
                case Some(m) if m.equalsIgnoreCase("pull") => Subscriber.subscribe(sub, rule, Pull(format)).map(Ok(_))
                case Some(m) if m.equalsIgnoreCase("push") =>
                  request.getQueryString("callback") match {
                    case Some(url) =>
                      Subscriber.subscribe(sub, rule, Push(getHandlerFor(format, url, timeContext))).map(Ok(_))
                    case None => Future.successful(BadRequest("missing callback for method push"))
                  }
                case _ => Future.successful(BadRequest("unsupported or missing method for real time search "))
              }
            case _ => Future.successful(BadRequest(s"un-recognized type: ${request.headers("format")}"))
          }
      }
    }.recover(asyncErrorHandler).get

  def getHandlerFor(format: FormatType, url: String, timeContext: Option[Long]): (Seq[String]) => Unit = { uuids => {

    uuids.foreach { uuid =>
      logger.debug(s"Sending $uuid to $url.")
    }
    val infotonsFut = crudServiceFS.getInfotonsByPathOrUuid(uuids = uuids.toVector)
    //TODO: probably not the best host to provide a formatter. is there a way to get the original host the subscription was asked from?
    val formatter =
      formatterManager.getFormatter(format, timeContext, s"http://${cmwell.util.os.Props.machineName}:9000")
    val futureRes = infotonsFut.flatMap { bag =>
      import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
      SimpleHttpClient.post[String](url, formatter.render(bag), Some(formatter.mimetype))
    }
    futureRes.onComplete {
      case Success(wsr) =>
        if (wsr.status != 200)
          logger.warn(s"bad response: ${wsr.status}, ${wsr.payload}")
      case Failure(t) => logger.error(s"post to $url failed", t)
    }
  }
  }

  def handleUnsubscribe(request: Request[AnyContent]): Future[Result] = {
    request.getQueryString("sub") match {
      case Some(sub) =>
        Subscriber.unsubscribe(sub)
        Future.successful(Ok(s"unsubscribe $sub"))
      case None =>
        Future.successful(BadRequest("missing sub param. "))
    }
  }
}
