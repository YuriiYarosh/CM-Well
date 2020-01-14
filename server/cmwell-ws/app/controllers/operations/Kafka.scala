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

import cmwell.common.CommandSerializer
import cmwell.web.ld.cmw.CMWellRDFHelper
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, FileInfotonCaching}
import javax.inject.Inject
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import play.api.mvc.{Action, AnyContent, InjectedController}
import security.{Admin, AuthUtils}
import wsutil.FormatterManager

import scala.concurrent.ExecutionContext

class Kafka @Inject()(authUtils: AuthUtils,
                      crudServiceFS: CRUDServiceFS,
                      cmwellRDFHelper: CMWellRDFHelper,
                      formatterManager: FormatterManager,
                      assetsMetadataProvider: AssetsMetadataProvider,
                      assetsConfigurationProvider: AssetsConfigurationProvider,
                      servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {
  def handleKafkaConsume(topic: String, partition: Int): Action[AnyContent] = Action { implicit req =>
    val isSysTopic = {
      val sysTopics = Set("persist_topic", "index_topic", "persist_topic.priority", "index_topic.priority")
      sysTopics(topic)
    }
    if (isSysTopic && !authUtils.isOperationAllowedForUser(Admin, authUtils.extractTokenFrom(req), evenForNonProdEnv = true))
      Forbidden("Consuming this topic requires an Admin token.")
    else {
      val offset = req.getQueryString("offset").fold(0L)(_.toLong)
      val maxLengthOpt = req.getQueryString("max-length").map(_.toLong)
      val isText = req.getQueryString("format").fold(false)(_ == "text")

      val source = crudServiceFS.consumeKafka(topic, partition, offset, maxLengthOpt)
      Ok.chunked(source.map { bytes => (
        if(isSysTopic)  CommandSerializer.decode(bytes).toString
        else if(isText) new String(bytes, StandardCharsets.UTF_8)
        else            bytes.mkString(",")
        ) + "\n"
      })
    }
  }
}
