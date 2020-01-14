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
import cmwell.domain.FieldValue
import cmwell.web.ld.cmw.CMWellRDFHelper
import cmwell.ws.Streams
import cmwell.ws.util.TypeHelpers.asBoolean
import com.typesafe.scalalogging.LazyLogging
import controllers.{AssetsConfigurationProvider, AssetsMetadataProvider, BulkScrollHandler, CachedSpa, FileInfotonCaching}
import filters.Attrs
import javax.inject.Inject
import logic.CRUDServiceFS
import logic.services.ServicesRoutesCache
import org.joda.time.DateTime
import play.api.mvc.{InjectedController, Result}
import security.AuthUtils
import wsutil.FormatterManager

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class STP @Inject()(bulkScrollHandler: BulkScrollHandler,
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

  def handleStpControl(agent: String) = Action.async(parse.raw) { implicit req =>

    val p = Promise[Result]()

    val infotonPath = s"/meta/sys/agents/sparql/$agent"
    val flag = req.getQueryString("enabled").flatMap(asBoolean).getOrElse(true)

    val tokenOpt = authUtils.extractTokenFrom(req)
    val modifier = req.attrs(Attrs.UserName)

    (authUtils.isOperationAllowedForUser(security.Admin, tokenOpt)) match {
      case true => {
        val infotons = crudServiceFS.getInfotonByPathAsync(infotonPath)

        infotons.onComplete{
          case Success(infotonBox) => {
            infotonBox.isEmpty match {
              case false => {
                val fieldsOption = infotonBox.head.fields
                val activeFlag = ("active" -> Set(FieldValue(flag)))

                val newFields = fieldsOption match {
                  case Some(fields) => fields + activeFlag
                  case None => Map(activeFlag)
                }

                val firstInfoton = infotonBox.head
                val newInfoton = firstInfoton.copyInfoton(firstInfoton.systemFields.copy(lastModified = new DateTime(System.currentTimeMillis),
                  lastModifiedBy = modifier), fields=Some(newFields))
                val deletes = Map(infotonPath ->  Map("active" -> None))

                crudServiceFS.upsertInfotons(inserts = List(newInfoton), deletes = deletes, deletesModifier = modifier).onComplete({
                  case Success(_)=> p.completeWith(Future.successful(Ok("""{"success":true}""")))
                  case Failure(ex) => {
                    logger.debug(ex.getMessage)
                    throw ex
                  }
                })
              }
              case _ =>  p.completeWith(Future.successful(NotFound(s"Config infoton: $infotonPath  not found")))
            }
          }
          case Failure(ex) => p.completeWith(Future.successful(NotFound(s"Config infoton: $infotonPath not found")))
        }
      }
      case _ => p.completeWith(Future.successful(Forbidden("Not allowed to use stp")))
    }

    p.future
  }
}
