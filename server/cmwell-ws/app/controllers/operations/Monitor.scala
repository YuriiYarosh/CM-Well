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

import actions.{CmwellRequest, GetRequest, RequestMonitor}
import akka.util.Timeout
import cmwell.util.string.Base64
import cmwell.web.ld.cmw.CMWellRDFHelper
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, FileInfotonCaching}
import javax.inject.Inject
import k.grid.Grid
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.mvc.InjectedController
import wsutil.FormatterManager

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

class Monitor @Inject()(crudServiceFS: CRUDServiceFS,
                        cmwellRDFHelper: CMWellRDFHelper,
                        formatterManager: FormatterManager,
                        assetsMetadataProvider: AssetsMetadataProvider,
                        assetsConfigurationProvider: AssetsConfigurationProvider,
                        servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {

  def startRequestMonitoring = Action {
    RequestMonitor.startMonitoring
    Ok("Started monitor requests")
  }

  def stopRequestMonitoring = Action {
    RequestMonitor.stopMonitoring
    Ok("Stopped monitor requests")
  }

  def requestDetailedView(path: String) = Action.async { implicit req =>
  {
    implicit val timeout = Timeout(2.seconds)
    import akka.pattern.ask
    val actorPath = Base64.decodeBase64String(path, "UTF-8")
    val f = (Grid.selectByPath(actorPath) ? GetRequest).mapTo[CmwellRequest]
    f.map { cmwReq =>
      Ok(s"""
            | Type: ${cmwReq.requestType}
            | Path: ${cmwReq.path}
            | Query: ${cmwReq.queryString}
            | Body: ${cmwReq.requestBody}
          """.stripMargin)
    }
      .recover {
        case t: Throwable => Ok("This request is not monitored anymore")
      }
  }
  }
}
