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

import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, FileInfotonCaching}
import javax.inject.Inject
import ld.cmw.passiveFieldTypesCacheImpl
import logic.CRUDServiceFS
import play.api.http.ContentTypes
import play.api.mvc.InjectedController

import scala.concurrent.ExecutionContext

class TypesCache @Inject()(crudServiceFS: CRUDServiceFS,
                            assetsMetadataProvider: AssetsMetadataProvider,
                            assetsConfigurationProvider: AssetsConfigurationProvider)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {


  lazy val typesCache: passiveFieldTypesCacheImpl = crudServiceFS.passiveFieldTypesCache

  def handleTypesCacheGet = Action(_ => Ok(typesCache.getState).as(ContentTypes.JSON))
}
