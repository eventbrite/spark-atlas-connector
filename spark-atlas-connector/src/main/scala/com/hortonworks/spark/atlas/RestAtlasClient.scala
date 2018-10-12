/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.atlas

import java.util

import scala.collection.JavaConverters._

import com.sun.jersey.core.util.MultivaluedMapImpl

import org.apache.atlas.AtlasClientV2
import org.apache.atlas.model.SearchFilter
import org.apache.atlas.model.instance.{AtlasEntity, AtlasEntityHeader}
import org.apache.atlas.model.instance.AtlasEntity.{AtlasEntitiesWithExtInfo, AtlasEntityWithExtInfo}
import org.apache.atlas.model.instance.EntityMutations.EntityOperation
import org.apache.atlas.model.typedef.AtlasTypesDef
import org.apache.atlas.utils.AuthenticationUtil

class RestAtlasClient(atlasClientConf: AtlasClientConf) extends AtlasClient {

  private val client = {
    if (!AuthenticationUtil.isKerberosAuthenticationEnabled) {
      val basicAuth = Array(atlasClientConf.get(AtlasClientConf.CLIENT_USERNAME),
        atlasClientConf.get(AtlasClientConf.CLIENT_PASSWORD))
      new AtlasClientV2(getServerUrl(), basicAuth)
    } else {
      new AtlasClientV2(getServerUrl(): _*)
    }
  }

  private def getServerUrl(): Array[String] = {

    atlasClientConf.getUrl(AtlasClientConf.ATLAS_REST_ENDPOINT.key) match {
      case a: util.ArrayList[_] => a.toArray().map(b => b.toString)
      case s: String => Array(s)
      case _: Throwable => throw new IllegalArgumentException(s"Fail to get atlas.rest.address")
    }
  }

  override def createAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = {
    client.createAtlasTypeDefs(typeDefs)
  }

  override def getAtlasTypeDefs(searchParams: MultivaluedMapImpl): AtlasTypesDef = {
    val searchFilter = new SearchFilter(searchParams)
    client.getAllTypeDefs(searchFilter)
  }

  override def updateAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = {
    client.updateAtlasTypeDefs(typeDefs)
  }

  override protected def doCreateEntities(entities: Seq[AtlasEntity]): Unit = {
    val entitesWithExtInfo = new AtlasEntitiesWithExtInfo()
    entities.foreach(entitesWithExtInfo.addEntity)
    val response = client.createEntities(entitesWithExtInfo)
    try {
      response.getCreatedEntities
      for ((event: EntityOperation, entities: java.util.List[AtlasEntityHeader]) <-
            response.getMutatedEntities.asScala) {
        event match {
          case EntityOperation.CREATE => logInfo(s"Created entities " +
            s"${entities.asScala.map(_.getGuid).mkString(", ")}")
          case EntityOperation.UPDATE => logInfo(s"Updated entities " +
            s"${entities.asScala.map(_.getGuid).mkString(", ")}")
          case EntityOperation.PARTIAL_UPDATE => logInfo(s"Partially updated entities " +
            s"${entities.asScala.map(_.getGuid).mkString(", ")}")
          case EntityOperation.DELETE => logWarn(s"Delete caught on a doCreateEntity call " +
            s"${entities.asScala.map(_.getGuid).mkString(", ")}")
          case _ => logWarn(s"Unhandled event: $event on entity: " +
            s"$entities")
        }
      }
    } catch {
      case _: Throwable => throw new IllegalStateException(s"Failed to get create entities: " +
        s"${response.toString}")
    }
  }

  override protected def doDeleteEntityWithUniqueAttr(
      entityType: String,
      attribute: String): Unit = {
    client.deleteEntityByAttribute(entityType,
        Map(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME -> attribute).asJava)
    logInfo(s"Deleted entity $attribute, type $entityType")
  }

  override protected def doUpdateEntityWithUniqueAttr(
      entityType: String,
      attribute: String,
      entity: AtlasEntity): Unit = {
    client.updateEntityByAttribute(
      entityType,
      Map(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME -> attribute).asJava,
      new AtlasEntityWithExtInfo(entity))
    logInfo(s"Updated entity with qualifiedName $attribute of type $entityType")
  }

  override def getAtlasEntitiesWithUniqueAttribute(
      entityType: String, attribute: String): AtlasEntity = {
    val entities = client.getEntityByAttribute(entityType,
      Map(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME -> attribute).asJava)
    entities.getReferredEntities.asScala.toSeq
    entities.getEntity
  }

  override def getAtlasEntityWithGuid(guid: String): AtlasEntity = {
    client.getEntityByGuid(guid).getEntity()
  }

  override def deleteAtlasEntitiesWithGuid(guid: String): Unit = {
    logDebug(s"Deleting entity: $guid")
    client.deleteEntityByGuid(guid)
  }

  override def deleteAtlasEntitiesWithGuidBulk(guid: Seq[String]): Unit = {
    logDebug(s"Deleting entities:\n${guid.mkString("\n")}")
    logDebug(s"java object converted to ${guid.asJava.getClass} \n"
    + s"and looks like ${guid.asJava}")
    try {
      client.deleteEntitiesByGuids(guid.asJava)
    } catch {
      case e: Exception => logWarn(s"Bulk delete failed", e)
    }
  }

  override def putEntityByGuid(atlasEntity: AtlasEntityWithExtInfo): Unit = {
    val updatedEntity = client.updateEntity(atlasEntity)
    logDebug(updatedEntity)
  }

  override def doSearchByDSL(qualifiedName: String, entityType: String): Seq[AtlasEntityHeader] = {
    val query = s"$entityType WHERE qualifiedName=$qualifiedName"
    val res = client.dslSearch(query).getEntities
    res.asScala
  }
}
