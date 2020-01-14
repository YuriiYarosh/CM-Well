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

import actions.ActiveInfotonGenerator
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cmwell.rts.Subscriber
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.Streams
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, BulkScrollHandler, CachedSpa, FileInfotonCaching}
import filters.Attrs
import javax.inject.Inject
import ld.cmw.passiveFieldTypesCacheImpl
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.mvc.{AnyContent, InjectedController, Request, Result}
import security.AuthUtils
import wsutil.{FormatterManager, extractFieldsMask}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

class Pull @Inject()(bulkScrollHandler: BulkScrollHandler,
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

  def handlePull(req: Request[AnyContent]): Future[Result] = {

    val timeContext = req.attrs.get(Attrs.RequestReceivedTimestamp)

    extractFieldsMask(req, typesCache, cmwellRDFHelper, timeContext).flatMap { fieldsMask =>
      req.getQueryString("sub") match {
        case Some(sub) => {
          val p = Promise[Unit]()
          val source = Source.unfoldAsync[String, ByteString](sub) { subscription =>
            Subscriber
              .pull(subscription)
              .flatMap(
                d =>
                  d.data match {
                    //              //IS FIRST CASE EVEN REACHABLE?!?!?!?
                    //              case _ if p.isCompleted => Future.successful(None)
                    case v if v.isEmpty =>
                      cmwell.util.concurrent.SimpleScheduler.schedule[Option[(String, ByteString)]](3.seconds)(
                        Some(subscription -> cmwell.ws.Streams.endln)
                      )
                    case _ => {
                      val formatter = formatterManager.getFormatter(
                        d.format,
                        timeContext,
                        req.host,
                        req.uri,
                        req.queryString.keySet("pretty"),
                        req.queryString.keySet("raw"),
                        req.queryString.get("callback").flatMap(_.headOption)
                      )

                      val f = crudServiceFS.getInfotonsByPathOrUuid(uuids = d.data).map { bag =>
                        val data = ByteString(
                          bag.infotons.map(i => formatter.render(i.masked(fieldsMask))).mkString("", "\n", "\n"),
                          StandardCharsets.UTF_8
                        )
                        Some(subscription -> data)
                      }
                      //                val f = Future(Some(subscription -> d.data.map(CRUDServiceFS.getInfotonByUuid).collect{
                      //                  case oi : Option[Infoton] if oi.isDefined => oi.get.masked(fieldsMask)
                      //                }.map(formatter.render).mkString("","\n","\n").getBytes("UTF-8")))
                      f.recover {
                        case e => {
                          logger.error("future failed in handlePull", e)
                          Option.empty[(String, ByteString)]
                        }
                      }
                    }
                  }
              )
          }

          Future.successful(Ok.chunked(source))
        }
        case None =>
          Future.successful(BadRequest("missing sub param."))
      }
    }
  }
}
