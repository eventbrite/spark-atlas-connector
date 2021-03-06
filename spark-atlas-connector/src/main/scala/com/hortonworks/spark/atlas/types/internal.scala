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

import com.hortonworks.spark.atlas.AtlasClientConf

import scala.collection.mutable
import scala.collection.JavaConverters._
import org.apache.atlas.AtlasClient
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.{Pipeline, PipelineModel}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

object internal extends AtlasEntityUtils with Logging {
  override val conf: AtlasClientConf = new AtlasClientConf

  val cachedObjects = new mutable.HashMap[String, Object]

  def sparkDbUniqueAttribute(db: String): String = SparkUtils.getUniqueQualifiedPrefix() + db

  def sparkDbToEntities(dbDefinition: CatalogDatabase): Seq[AtlasEntity] = {
    val dbEntity = new AtlasEntity(metadata.DB_TYPE_STRING)
    val pathEntity = external.pathToEntity(dbDefinition.locationUri.toString)

    dbEntity.setAttribute(
      AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, sparkDbUniqueAttribute(dbDefinition.name))
    dbEntity.setAttribute("name", dbDefinition.name)
    dbEntity.setAttribute("description", dbDefinition.description)
    dbEntity.setAttribute("locationUri", pathEntity)
    dbEntity.setAttribute("properties", dbDefinition.properties.asJava)
    Seq(dbEntity, pathEntity)
  }

  def sparkStorageFormatUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table.storageFormat"
  }

  def sparkStorageFormatToEntities(
      storageFormat: CatalogStorageFormat,
      db: String,
      table: String): Seq[AtlasEntity] = {
    val sdEntity = new AtlasEntity(metadata.STORAGEDESC_TYPE_STRING)
    val pathEntity = storageFormat.locationUri.map { u => external.pathToEntity(u.toString) }

    sdEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      sparkStorageFormatUniqueAttribute(db, table))
    pathEntity.foreach { e => sdEntity.setAttribute("locationUri", e) }
    storageFormat.inputFormat.foreach(sdEntity.setAttribute("inputFormat", _))
    storageFormat.outputFormat.foreach(sdEntity.setAttribute("outputFormat", _))
    storageFormat.serde.foreach(sdEntity.setAttribute("serde", _))
    sdEntity.setAttribute("compressed", storageFormat.compressed)
    sdEntity.setAttribute("properties", storageFormat.properties.asJava)

    Seq(Some(sdEntity), pathEntity)
      .filter(_.isDefined)
      .flatten
  }

  def sparkColumnUniqueAttribute(db: String, table: String, col: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table.col-$col"
  }

  def sparkSchemaToEntities(schema: StructType, db: String, table: String): List[AtlasEntity] = {
    schema.map { struct =>
      val entity = new AtlasEntity(metadata.COLUMN_TYPE_STRING)

      entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        sparkColumnUniqueAttribute(db, table, struct.name))
      entity.setAttribute("name", struct.name)
      entity.setAttribute("type", struct.dataType.typeName)
      entity.setAttribute("nullable", struct.nullable)
      entity.setAttribute("metadata", struct.metadata.toString())
      entity
    }.toList
  }

  def sparkTableUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table@$clusterName"
  }

  def sparkTableToEntities(
      tableDefinition: CatalogTable,
      mockDbDefinition: Option[CatalogDatabase] = None): Seq[AtlasEntity] = {
    val db = tableDefinition.identifier.database.getOrElse("default")
    val dbDefinition = mockDbDefinition
      .getOrElse(SparkUtils.getExternalCatalog().getDatabase(db))

    val dbEntities = sparkDbToEntities(dbDefinition)
    val sdEntities =
      sparkStorageFormatToEntities(tableDefinition.storage, db, tableDefinition.identifier.table)
    val schemaEntities =
      sparkSchemaToEntities(tableDefinition.schema, db, tableDefinition.identifier.table)

    val tblEntity = new AtlasEntity(metadata.TABLE_TYPE_STRING)

    tblEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      sparkTableUniqueAttribute(db, tableDefinition.identifier.table))
    tblEntity.setAttribute("name", tableDefinition.identifier.table)
    tblEntity.setAttribute("database", dbEntities.head)
    tblEntity.setAttribute("tableType", tableDefinition.tableType.name)
    tblEntity.setAttribute("storage", sdEntities.head)
    tblEntity.setAttribute("schema", schemaEntities.asJava)
    tableDefinition.provider.foreach(tblEntity.setAttribute("provider", _))
    tblEntity.setAttribute("partitionColumnNames", tableDefinition.partitionColumnNames.asJava)
    tableDefinition.bucketSpec.foreach(
      b => tblEntity.setAttribute("bucketSpec", b.toLinkedHashMap.asJava))
    tblEntity.setAttribute("owner", tableDefinition.owner)
    tblEntity.setAttribute("createTime", tableDefinition.createTime)
    tblEntity.setAttribute("lastAccessTime", tableDefinition.lastAccessTime)
    tblEntity.setAttribute("properties", tableDefinition.properties.asJava)
    tableDefinition.comment.foreach(tblEntity.setAttribute("comment", _))
    tblEntity.setAttribute("unsupportedFeatures", tableDefinition.unsupportedFeatures.asJava)

    logDebug(s"SparkTableToEntities, $db, ${tableDefinition.identifier.table}: "
      + (Seq(tblEntity) ++ dbEntities ++ sdEntities ++ schemaEntities).toString)
    Seq(tblEntity) ++ dbEntities ++ sdEntities ++ schemaEntities
  }

  // ================ ML related entities ==================
  def mlDirectoryToEntity(uri: String, directory: String): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_DIRECTORY_TYPE_STRING)

    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, s"$uri.$directory")
    entity.setAttribute("name", s"$uri.$directory")
    entity.setAttribute("uri", uri)
    entity.setAttribute("directory", directory)
    entity
  }

  def mlPipelineToEntity(pipeline: Pipeline, directory: AtlasEntity): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_PIPELINE_TYPE_STRING)

    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, pipeline.uid)
    entity.setAttribute("name", pipeline.uid)
    entity.setAttribute("directory", directory)
    entity
  }

  def mlModelToEntity(model: PipelineModel, directory: AtlasEntity): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_MODEL_TYPE_STRING)

    val uid = model.uid.replaceAll("pipeline", "model")
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, uid)
    entity.setAttribute("name", uid)
    entity.setAttribute("directory", directory)
    entity
  }

  def mlFitProcessToEntity(
      pipeline: Pipeline,
      pipelineEntity: AtlasEntity,
      inputs: List[AtlasEntity],
      outputs: List[AtlasEntity]): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_FIT_PROCESS_TYPE_STRING)

    val uid = pipeline.uid.replaceAll("pipeline", "fit_process")
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, uid)
    entity.setAttribute("name", uid)
    entity.setAttribute("pipeline", pipelineEntity)
    entity.setAttribute("inputs", inputs.asJava)  // Dataset and Pipeline entity
    entity.setAttribute("outputs", outputs.asJava)  // ML model entity
    entity
  }

  def mlTransformProcessToEntity(
      model: PipelineModel,
      modelEntity: AtlasEntity,
      inputs: List[AtlasEntity],
      outputs: List[AtlasEntity]): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_TRANSFORM_PROCESS_TYPE_STRING)

    val uid = model.uid.replaceAll("pipeline", "transform_process")
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, uid)
    entity.setAttribute("name", uid)
    entity.setAttribute("model", modelEntity)
    entity.setAttribute("inputs", inputs.asJava)  // Dataset and Model entity
    entity.setAttribute("outputs", outputs.asJava)  // Dataset entity
    entity
  }

  def sparkProcessUniqueAttribute(db: String, table: String): String = {
    s"spark_process$db.$table@$clusterName"
  }

  // Spark Process Entity
  def etlProcessToEntity(
      inputs: List[AtlasEntity],
      outputs: List[AtlasEntity],
      logMap: Map[String, String]): AtlasEntity = {
    val entity = new AtlasEntity(metadata.PROCESS_TYPE_STRING)
    val appId = SparkUtils.sparkSession.sparkContext.applicationId
    val dbName = logMap.get("database")
    val tableName = outputs.head.getAttribute("name").toString
    val outputName = sparkProcessUniqueAttribute(dbName.get, tableName)

    val appName = SparkUtils.sparkSession.sparkContext.appName match {
      case "Spark shell" => s"Spark Shell $appId - $outputName"
      case "PySparkShell" => s"Spark Shell $appId - $outputName"
      case sql if sql.matches("SparkSQL::.*") => s"Spark SQL $appId - $outputName"
      case default => default + s" - $outputName"
    }
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      outputName) // Qualified Name
    entity.setAttribute("name", appName)
    entity.setAttribute("currUser", SparkUtils.currUser())
    entity.setAttribute("inputs", inputs.asJava)  // Dataset and Model entity
    entity.setAttribute("outputs", outputs.asJava)  // Dataset entity
    logMap.foreach { case (k, v) => entity.setAttribute(k, v)}
    logInfo("Created spark_process entity")
    logDebug("ETLProcessToEntityOutputs: " + outputs.mkString(", "))
    entity
  }

  def updateMLProcessToEntity(
      inputs: Seq[AtlasEntity],
      outputs: Seq[AtlasEntity],
      logMap: Map[String, String]): Seq[AtlasEntity] = {

    val model_uid = internal.cachedObjects("model_uid").asInstanceOf[String]
    val modelEntity = internal.cachedObjects(s"${model_uid}_modelEntity").
      asInstanceOf[AtlasEntity]
    val modelDirEntity = internal.cachedObjects(s"${model_uid}_modelDirEntity").
      asInstanceOf[AtlasEntity]

    if (internal.cachedObjects.contains("fit_process")) {

      // spark ml fit process
      val processEntity = internal.etlProcessToEntity(
        List(inputs.head), List(outputs.head), logMap)

      (Seq(processEntity, modelDirEntity, modelEntity)
        ++ inputs ++ outputs)
    } else {
      val new_inputs = List(inputs.head, modelDirEntity, modelEntity)

      // spark ml fit and score process
      val processEntity = internal.etlProcessToEntity(
        new_inputs, List(outputs.head), logMap)

      (Seq(processEntity, modelDirEntity, modelEntity)
        ++ new_inputs ++ outputs)
    }
  }
}
