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

import cmwell.domain.{FReference, FString, FieldValue, ObjectInfoton, SystemFields, VirtualInfoton}
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.Settings
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, FileInfotonCaching}
import javax.inject.Inject
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import org.joda.time.{DateTime, DateTimeZone}
import play.api.mvc.{InjectedController, Action}

import wsutil.FormatterManager

import scala.concurrent.ExecutionContext

class MetaQuad @Inject()(crudServiceFS: CRUDServiceFS,
                         cmwellRDFHelper: CMWellRDFHelper,
                         formatterManager: FormatterManager,
                         assetsMetadataProvider: AssetsMetadataProvider,
                         assetsConfigurationProvider: AssetsConfigurationProvider,
                         servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {
    
    def handleMetaQuad(base64OfPartition: String) = Action.async { req =>
      val underscorePartition = cmwell.util.string.Base64.decodeBase64String(base64OfPartition, "UTF-8") // e.g: "_2"
      val partition = underscorePartition.tail.toInt
      val iOpt = Some(
        VirtualInfoton(
          ObjectInfoton(SystemFields(
            s"/meta/quad/Y213ZWxsOi8vbWV0YS9zeXMjcGFydGl0aW9u$base64OfPartition",
            new DateTime(DateTimeZone.UTC),
            "VirtualInfoton",
            Settings.dataCenter,
            None,
            "http",
            ""),
            Map[String, Set[FieldValue]]("alias" -> Set(FString(s"partition_$partition")),
              "graph" -> Set(FReference(s"cmwell://meta/sys#partition_$partition"))))
        )
      )
      Read.infotonOptionToReply(req, iOpt.map(VirtualInfoton.v2i))
    }
}
