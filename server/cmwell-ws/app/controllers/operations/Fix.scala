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

import akka.util.ByteString
import cmwell.domain.SimpleResponse
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.{Settings, Streams}
import cmwell.ws.util.TypeHelpers.asInt
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, FileInfotonCaching}
import javax.inject.Inject
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.mvc.{AnyContent, InjectedController, Request, Result}
import security.AuthUtils
import wsutil.{FormatterManager, getFormatter, keepAliveByDrippingNewlines, normalizePath, overrideMimetype}

import scala.concurrent.{ExecutionContext, Future}

class Fix @Inject()(authUtils: AuthUtils,
                    crudServiceFS: CRUDServiceFS,
                    cmwellRDFHelper: CMWellRDFHelper,
                    formatterManager: FormatterManager,
                    assetsMetadataProvider: AssetsMetadataProvider,
                    assetsConfigurationProvider: AssetsConfigurationProvider,
                    servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {{
  def handleFix(req: Request[AnyContent]): Future[Result] = {
    if (!authUtils.isOperationAllowedForUser(security.Overwrite, authUtils.extractTokenFrom(req)))
      Future.successful(Forbidden("not authorized to overwrite"))
    else if (isReactive(req)) {
      val formatter = getFormatter(req, formatterManager, "json")
      val parallelism = req.getQueryString("parallelism").flatMap(asInt).getOrElse(1)
      crudServiceFS.rFix(normalizePath(req.path), parallelism).map { source =>
        val s = source
          .map { bs =>
            val msg = if (bs._2.isEmpty) None else Some(bs._2)
            ByteString(formatter.render(SimpleResponse(bs._1, msg)), StandardCharsets.UTF_8)
          }
          .intersperse(ByteString.empty, Streams.endln, Streams.endln)
        Ok.chunked(s)
      }
    } else {
      val limit = req.getQueryString("versions-limit").flatMap(asInt).getOrElse(Settings.defaultLimitForHistoryVersions)
      val f = crudServiceFS.fix(normalizePath(req.path), limit)
      val formatter = getFormatter(req, formatterManager, "json")
      val r = f.map { bs =>
        Ok(formatter.render(SimpleResponse(bs._1, if (bs._2.isEmpty) None else Some(bs._2))))
          .as(overrideMimetype(formatter.mimetype, req)._2)
      }
      keepAliveByDrippingNewlines(r)
    }
  }
}
