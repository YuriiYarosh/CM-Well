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
import cmwell.domain.SimpleResponse
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.{Settings, Streams}
import cmwell.ws.util.TypeHelpers.asInt
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, BulkScrollHandler, CachedSpa, FileInfotonCaching}
import javax.inject.Inject
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.mvc.{AnyContent, InjectedController, Request, Result}
import security.AuthUtils
import wsutil.{FormatterManager, getFormatter, normalizePath, overrideMimetype}

import scala.concurrent.{ExecutionContext, Future}

class Verify @Inject()(bulkScrollHandler: BulkScrollHandler,
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

  def handleVerify(req: Request[AnyContent]): Future[Result] = {
    val limit = req.getQueryString("versions-limit").flatMap(asInt).getOrElse(Settings.defaultLimitForHistoryVersions)
    val f = crudServiceFS.verify(normalizePath(req.path), limit)
    val formatter = getFormatter(req, formatterManager, "json")
    f.map { b =>
      Ok(formatter.render(SimpleResponse(b, None))).as(overrideMimetype(formatter.mimetype, req)._2)
    }
  }
}
