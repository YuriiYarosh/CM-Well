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

import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.Settings
import cmwell.ws.util.TypeHelpers.asInt
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, FileInfotonCaching}
import javax.inject.Inject
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.mvc.{AnyContent, InjectedController, Request, Result}
import wsutil.{FormatterManager, normalizePath, overrideMimetype}

import scala.concurrent.{ExecutionContext, Future}

class Info @Inject()(crudServiceFS: CRUDServiceFS,
                     cmwellRDFHelper: CMWellRDFHelper,
                     formatterManager: FormatterManager,
                     assetsMetadataProvider: AssetsMetadataProvider,
                     assetsConfigurationProvider: AssetsConfigurationProvider,
                     servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {
  def handleInfo(req: Request[AnyContent]): Future[Result] = {
    val path = normalizePath(req.path)
    val limit = req.getQueryString("versions-limit").flatMap(asInt).getOrElse(Settings.defaultLimitForHistoryVersions)
    crudServiceFS
      .info(path, limit)
      .map {
        case (cas, es, zs) => {
          val c = cas.map {
            case (u, io) => s"cas $u ${io.map(i => formatterManager.jsonFormatter.render(i)).getOrElse("none")}"
          }
          val e = es.groupBy(_._1).map {
            case (uuid, tuples) =>
              val (indices, sources) = tuples.unzip {
                case (_, index, version, source) =>
                  s"$index($version)" -> source
              }
              val start = s"es  $uuid ${indices.mkString("[", ",", "]")} "
              val head = start + sources.head
              if (sources.size == 1) head
              else {
                val spaces = " " * start.length
                head + sources.mkString("\n" + spaces)
              }
          }
          val z = zs.map("zs  ".+)
          val body = (c ++ e ++ z).sortBy(_.drop(4)).mkString("\n")
          Ok(body).as(overrideMimetype("text/plain;charset=UTF8", req)._2)
        }
      }
      .recover {
        case e: Throwable => {
          logger.error("x-info future failed", e)
          InternalServerError(e.getMessage)
        }
      }
  }
}
