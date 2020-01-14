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
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.Streams
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, BulkScrollHandler, CachedSpa, FileInfotonCaching}
import javax.inject.Inject
import k.grid.{ClientActor, Grid, GridJvm, RestartJvm}
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.mvc.InjectedController
import security.AuthUtils
import wsutil.FormatterManager

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

class PoisonPill @Inject()(bulkScrollHandler: BulkScrollHandler,
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

  def handlePoisonPill() = Action { implicit req =>
    if (!Auth.isAdminEvenNonProd(req)) {
      Forbidden("Not authorized")
    } else {
      val hostOpt = req.getQueryString("host")
      val jvmOpt = req.getQueryString("jvm")

      // retain the old behavior
      if (hostOpt.isEmpty || jvmOpt.isEmpty) {
        logger.info("Got poison pill. Will now exit.")
        cmwell.util.concurrent.SimpleScheduler.schedule(1 seconds) {
          sys.exit(1)
        }
        Ok("Goodbye")
      } else {
        val host = hostOpt.get
        val jvm = jvmOpt.get

        Grid.selectActor(ClientActor.name, GridJvm(host, jvm)) ! RestartJvm

        Ok(s"Restarted $jvm at $host")
      }
    }
  }
}
