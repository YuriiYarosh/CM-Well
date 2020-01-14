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
import logic.services.ServicesRoutesCache
import play.api.mvc.InjectedController

import scala.concurrent.{ExecutionContext, Future}

class ServicesCache @Inject()(assetsMetadataProvider: AssetsMetadataProvider,
                              assetsConfigurationProvider: AssetsConfigurationProvider,
                              servicesRoutesCache: ServicesRoutesCache)(implicit ec: ExecutionContext)
  extends FileInfotonCaching(assetsMetadataProvider.get, assetsConfigurationProvider.get)
    with InjectedController
    with LazyLogging {

  def handleServicesRoutesCacheGet = Action.async (req => {
    if (req.getQueryString("op").fold(false)(_ == "refresh"))
      servicesRoutesCache.populate().map(_ => Ok("""{"success":true}"""))
    else
      Future.successful(Ok(servicesRoutesCache.list.mkString("\n")))
  })
}
