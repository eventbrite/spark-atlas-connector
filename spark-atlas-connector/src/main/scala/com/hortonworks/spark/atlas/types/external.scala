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

package com.hortonworks.spark.atlas.types

import java.io.File
import java.net.{URI, URISyntaxException}
import java.util.Date

import scala.collection.JavaConverters._
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}
import org.apache.atlas.`type`.AtlasTypeUtil
import org.apache.atlas.{AtlasClient, AtlasConstants}
import org.apache.atlas.hbase.bridge.HBaseAtlasHook._
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.commons.lang.RandomStringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.types.StructType

object external extends Logging {
  // External metadata types used to link with external entities

  // ================ File system entities ======================
  val FS_PATH_TYPE_STRING = "fs_path"
  val HDFS_PATH_TYPE_STRING = "hdfs_path"

  def pathToEntity(path: String): AtlasEntity = {
    val uri = resolveURI(path)
    val entity = if (uri.getScheme == "hdfs") {
      new AtlasEntity(HDFS_PATH_TYPE_STRING)
    } else {
      new AtlasEntity(FS_PATH_TYPE_STRING)
    }

    val fsPath = new Path(uri)
    entity.setAttribute(AtlasClient.NAME,
      Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
    entity.setAttribute("path", Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, uri.toString)
    if (uri.getScheme == "hdfs") {
      entity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, uri.getAuthority)
    }

    entity
  }

  def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
  }

  // ================ HBase entities ======================
  val HBASE_NAMESPACE_STRING = "hbase_namespace"
  val HBASE_TABLE_STRING = "hbase_table"
  val HBASE_COLUMNFAMILY_STRING = "hbase_column_family"
  val HBASE_COLUMN_STRING = "hbase_column"

  def hbaseTableToEntity(cluster: String, tableName: String, nameSpace: String)
      : Seq[AtlasEntity] = {
    val hbaseEntity = new AtlasEntity(HBASE_TABLE_STRING)

    hbaseEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      getTableQualifiedName(cluster, nameSpace, tableName))
    hbaseEntity.setAttribute(AtlasClient.NAME, tableName.toLowerCase)
    hbaseEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, cluster)
    hbaseEntity.setAttribute("uri", nameSpace.toLowerCase + ":" + tableName.toLowerCase)
    Seq(hbaseEntity)
  }

  // ================ Kafka entities =======================
  val KAFKA_TOPIC_STRING = "kafka_topic"

  def kafkaToEntity(cluster: String, topicName: String): Seq[AtlasEntity] = {
    val kafkaEntity = new AtlasEntity(KAFKA_TOPIC_STRING)

    kafkaEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      topicName.toLowerCase + '@' + cluster)
    kafkaEntity.setAttribute(AtlasClient.NAME, topicName.toLowerCase)
    kafkaEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, cluster)
    kafkaEntity.setAttribute("uri", topicName.toLowerCase)
    kafkaEntity.setAttribute("topic", topicName.toLowerCase)
    Seq(kafkaEntity)
  }

  // ================== Hive entities =====================
  val HIVE_DB_TYPE_STRING = "hive_db"
  val HIVE_STORAGEDESC_TYPE_STRING = "hive_storagedesc"
  val HIVE_COLUMN_TYPE_STRING = "hive_column"
  val HIVE_TABLE_TYPE_STRING = "hive_table"

  def hiveDbUniqueAttribute(cluster: String, db: String): String = s"${db.toLowerCase}@$cluster"

  def hiveDbToEntities(dbDefinition: CatalogDatabase, cluster: String): Seq[AtlasEntity] = {
    val dbEntity = new AtlasEntity(HIVE_DB_TYPE_STRING)

    dbEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      hiveDbUniqueAttribute(cluster, dbDefinition.name.toLowerCase))
    dbEntity.setAttribute(AtlasClient.NAME, dbDefinition.name.toLowerCase)
    dbEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, cluster)
    dbEntity.setAttribute("description", dbDefinition.description)
    dbEntity.setAttribute("location", dbDefinition.locationUri.toString)
    dbEntity.setAttribute("parameters", dbDefinition.properties.asJava)
    Seq(dbEntity)
  }

  def hiveStorageDescUniqueAttribute(
      cluster: String,
      db: String,
      table: String,
      isTempTable: Boolean = false): String = {
    hiveTableUniqueAttribute(cluster, db, table, isTempTable) + "_storage"
  }

  def hiveStorageDescToEntities(
      storageFormat: CatalogStorageFormat,
      cluster: String,
      db: String,
      table: String,
      isTempTable: Boolean = false): Seq[AtlasEntity] = {
    val sdEntity = new AtlasEntity(HIVE_STORAGEDESC_TYPE_STRING)

    sdEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      hiveStorageDescUniqueAttribute(cluster, db, table, isTempTable))
    storageFormat.inputFormat.foreach(sdEntity.setAttribute("inputFormat", _))
    storageFormat.outputFormat.foreach(sdEntity.setAttribute("outputFormat", _))
    sdEntity.setAttribute("compressed", storageFormat.compressed)
    sdEntity.setAttribute("parameters", storageFormat.properties.asJava)
    storageFormat.serde.foreach(sdEntity.setAttribute(AtlasClient.NAME, _))
    storageFormat.locationUri.foreach { u => sdEntity.setAttribute("location", u.toString) }
    Seq(sdEntity)
  }

  def hiveColumnUniqueAttribute(
      cluster: String,
      db: String,
      table: String,
      column: String,
      isTempTable: Boolean = false): String = {
    val tableName = hiveTableUniqueAttribute(cluster, db, table, isTempTable)
    val parts = tableName.split("@")
    s"${parts(0)}.${column.toLowerCase}@${parts(1)}"
  }

  def hiveSchemaToEntities(
      schema: StructType,
      cluster: String,
      db: String,
      table: String,
      isTempTable: Boolean = false): List[AtlasEntity] = {

    schema.map { struct =>
      val entity = new AtlasEntity(HIVE_COLUMN_TYPE_STRING)

      entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        hiveColumnUniqueAttribute(cluster, db, table, struct.name, isTempTable))
      entity.setAttribute(AtlasClient.NAME, struct.name)
      entity.setAttribute("type", struct.dataType.typeName)
      entity.setAttribute("comment", struct.getComment())
      entity
    }.toList
  }

  def hiveTableUniqueAttribute(
      cluster: String,
      db: String,
      table: String,
      isTemporary: Boolean = false): String = {
    val tableName = if (isTemporary) {
      if (SessionState.get() != null && SessionState.get().getSessionId != null) {
        s"${table}_temp-${SessionState.get().getSessionId}"
      } else {
        s"${table}_temp-${RandomStringUtils.random(10)}"
      }
    } else {
      table
    }

    s"${db.toLowerCase}.${tableName.toLowerCase}@$cluster"
  }

  def hiveTableToEntities(
      tableDefinition: CatalogTable,
      cluster: String,
      mockDbDefinition: Option[CatalogDatabase] = None): Seq[AtlasEntity] = {
    val database = tableDefinition.identifier.database.getOrElse("default")
    val table = tableDefinition.identifier.table
    val databaseDefinition =
      mockDbDefinition.getOrElse(SparkUtils.getExternalCatalog().getDatabase(database))

    val databaseEntities = hiveDbToEntities(databaseDefinition, cluster)
    val storagedescriptorEntities =
      hiveStorageDescToEntities(tableDefinition.storage, cluster, database, table)
    val tableEntity = new AtlasEntity(HIVE_TABLE_TYPE_STRING)

    tableEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      hiveTableUniqueAttribute(cluster, database, table))
    tableEntity.setAttribute(AtlasClient.NAME, table)
    tableEntity.setAttribute(AtlasClient.OWNER, tableDefinition.owner)
    tableEntity.setAttribute("createTime", new Date(tableDefinition.createTime))
    tableEntity.setAttribute("lastAccessTime", new Date(tableDefinition.lastAccessTime))
    tableDefinition.comment.foreach(tableEntity.setAttribute("comment", _))
    tableEntity.setAttribute("db", databaseEntities.head)
    tableEntity.setAttribute("sd", storagedescriptorEntities.head)
    tableEntity.setAttribute("parameters", tableDefinition.properties.asJava)
    tableDefinition.viewText.foreach(tableEntity.setAttribute("viewOriginalText", _))
    tableEntity.setAttribute("tableType", tableDefinition.tableType.name)

    val schemaEntities = hiveSchemaToEntities(
      tableDefinition.schema, cluster, database, table).map { entity =>
      entity.setAttribute("table", AtlasTypeUtil.getAtlasObjectId(tableEntity))
      entity
    }
    tableEntity.setAttribute("columns", schemaEntities.asJava)

    Seq(tableEntity) ++ databaseEntities ++ storagedescriptorEntities ++ schemaEntities
  }
}
