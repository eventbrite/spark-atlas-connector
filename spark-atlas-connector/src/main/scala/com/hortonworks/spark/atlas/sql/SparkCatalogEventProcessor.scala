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

package com.hortonworks.spark.atlas.sql

import java.util.{ArrayList, LinkedHashMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.catalog._
import com.hortonworks.spark.atlas.{AbstractEventProcessor, AtlasClient, AtlasClientConf}
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

class SparkCatalogEventProcessor(
    private[atlas] val atlasClient: AtlasClient,
    val conf: AtlasClientConf)
  extends AbstractEventProcessor[ExternalCatalogEvent] with AtlasEntityUtils with Logging {

  override protected def process(catalogEvent: ExternalCatalogEvent): Unit = {
    catalogEvent match {
      case CreateDatabaseEvent(db) =>
        val dbDefinition = SparkUtils.getExternalCatalog().getDatabase(db)
        val entities = dbToEntities(dbDefinition)
        atlasClient.createEntities(entities)
        logInfo(s"Created db entity $db")

      case DropDatabaseEvent(db) =>
        atlasClient.deleteEntityWithUniqueAttr(external.HIVE_DB_TYPE_STRING, dbUniqueAttribute(db))
        logInfo(s"Deleted db entity $db")

      // TODO. We should also not create/alter view table in Atlas
      case CreateTableEvent(db, table) =>
        val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
        val tableEntities = tableToEntities(tableDefinition)
        atlasClient.createEntities(tableEntities)
        logInfo(s"Created table entity $table")

      case DropTableEvent(db, table) =>
        val isHiveTable: Boolean = true

        // If soft deletes are enabled, we need to manually drop the columns
        if (conf.get(AtlasClientConf.ATLAS_DELETEHANDLER) ==
          "org.apache.atlas.repository.store.graph.v1.SoftDeleteHandlerV1") {
          val tableDefinition: AtlasEntity = atlasClient.getAtlasEntitiesWithUniqueAttribute(
            tableType(isHiveTable), tableUniqueAttribute(db, table, isHiveTable))
          val guidBuffer: ListBuffer[String] = ListBuffer()


          if (!tableDefinition.hasAttribute("columns")) {
            logInfo(s"No columns found for $db.$table, continuing without dropping any columns")
          } else {
            val columnsMap = tableDefinition.getAttribute("columns")
              .asInstanceOf[ArrayList[LinkedHashMap[String, String]]]
            columnsMap.asScala.toList.foreach { x =>
              if (x.get("typeName") == "hive_column") {
                logDebug(s"Adding ${x.get("guid")} to guidBuffer for DropTableEvent")
                guidBuffer += x.get("guid")
              }
            }
            atlasClient.deleteAtlasEntitiesWithGuidBulk(guidBuffer.toList)
          }
        }

        // Drop storage descriptor and table
        atlasClient.deleteEntityWithUniqueAttr(
          tableType(isHiveTable), tableUniqueAttribute(db, table, isHiveTable))
        atlasClient.deleteEntityWithUniqueAttr(
          storageFormatType(isHiveTable), storageFormatUniqueAttribute(db, table, isHiveTable))
        logInfo(s"Deleted table entity $table")

      case RenameTableEvent(db, name, newName) =>
        val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, newName)
        val isHiveTbl = isHiveTable(tableDefinition)
        // Update storageFormat's unique attribute
        val sdEntity = new AtlasEntity(storageFormatType(isHiveTbl))
        sdEntity.setAttribute(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
          storageFormatUniqueAttribute(db, newName, isHiveTbl))

        atlasClient.updateEntityWithUniqueAttr(
          storageFormatType(isHiveTbl),
          storageFormatUniqueAttribute(db, name, isHiveTbl),
          sdEntity)
        logInfo(s"Renamed $db.$name to $db.$newName")

        // Update column's unique attribute
        tableDefinition.schema.foreach { sf =>
          val colEntity = new AtlasEntity(columnType(isHiveTbl))
          colEntity.setAttribute(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
            columnUniqueAttribute(db, newName, sf.name, isHiveTbl))
          atlasClient.updateEntityWithUniqueAttr(
            columnType(isHiveTbl),
            columnUniqueAttribute(db, name, sf.name, isHiveTbl),
            colEntity)
        }
        logInfo(s"Updated column attributes for renamed table $db.$newName")

        // Update Table name and Table's unique attribute
        val tableEntity = new AtlasEntity(tableType(isHiveTbl))
        tableEntity.setAttribute(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
          tableUniqueAttribute(db, newName, isHiveTbl))
        tableEntity.setAttribute("name", newName)
        atlasClient.updateEntityWithUniqueAttr(
          tableType(isHiveTbl),
          tableUniqueAttribute(db, name, isHiveTbl),
          tableEntity)
        logInfo(s"Updated table attributes for renamed table $db.$newName")

      case AlterDatabaseEvent(db) =>
        val dbDefinition = SparkUtils.getExternalCatalog().getDatabase(db)
        val dbEntities = dbToEntities(dbDefinition)
        atlasClient.createEntities(dbEntities)
        logInfo(s"Updated DB properties for $db")

      case AlterTableEvent(db, table, kind) =>
        val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
        kind match {
          case "table" =>
            val tableEntities = tableToEntities(tableDefinition)
            atlasClient.createEntities(tableEntities)
            logInfo(s"Updated table entity $db.$table")

          case "dataSchema" =>
            val isHiveTbl = isHiveTable(tableDefinition)
            val schemaEntities =
              schemaToEntities(tableDefinition.schema, db, table, isHiveTbl)
            atlasClient.createEntities(schemaEntities)

            val tableEntity = new AtlasEntity(tableType(isHiveTbl))
            tableEntity.setAttribute("schema", schemaEntities.asJava)
            atlasClient.updateEntityWithUniqueAttr(
              tableType(isHiveTbl),
              tableUniqueAttribute(db, table, isHiveTbl),
              tableEntity)
            logInfo(s"Updated schema for $db.$table")

          case "stats" =>
            logDebug(s"Stats update will not be tracked here")

          case _ =>
          // No op.
            logWarn(s"Unhandled Alter for $db.$table, kind: $kind")
        }

      // Pass unused events
      case CreateDatabasePreEvent(db) =>
      case DropDatabasePreEvent(db) =>
      case AlterDatabasePreEvent(db) =>
      case CreateTablePreEvent(db, table) =>
      case DropTablePreEvent(db, table) =>
      case AlterTablePreEvent(db, name, kind) =>
      case RenameTablePreEvent(db, name, newName) =>
      case CreateFunctionPreEvent(db, name) =>
      case CreateFunctionEvent(db, name) =>
      case DropFunctionPreEvent(db, name) =>
      case DropFunctionEvent(db, name) =>
      case AlterFunctionPreEvent(db, name) =>
      case AlterFunctionEvent(db, name) =>

      case f =>
        logWarn(s"Drop unknown event $f")
    }
  }
}
