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

import java.nio.file.Files

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

import com.sun.jersey.core.util.MultivaluedMapImpl
import com.hortonworks.spark.atlas.{AtlasClient, AtlasClientConf, TestUtils}
import com.hortonworks.spark.atlas.utils.SparkUtils

import org.apache.atlas.model.instance.{AtlasEntity, AtlasEntityHeader}
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo
import org.apache.atlas.model.typedef.AtlasTypesDef
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.{LongType, StructType}
import org.scalatest.concurrent.Eventually._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SparkCatalogEventProcessorSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  import TestUtils._

  private var sparkSession: SparkSession = _
  private val atlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession = null

    super.afterAll()
  }

  test("correctly handle DB related events") {
    val processor =
      new SparkCatalogEventProcessor(new FirehoseAtlasClient(atlasClientConf), atlasClientConf)
    processor.startThread()

    var atlasClient: FirehoseAtlasClient = null
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      processor.atlasClient should not be (null)
      atlasClient = processor.atlasClient.asInstanceOf[FirehoseAtlasClient]
    }

    val tempPath = Files.createTempDirectory("db_")
    val dbDefinition = createDB("db1", tempPath.normalize().toUri.toString)
    SparkUtils.getExternalCatalog().createDatabase(dbDefinition, ignoreIfExists = true)
    processor.pushEvent(CreateDatabaseEvent("db1"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.createEntityCall.size > 0)
      assert(atlasClient.createEntityCall(processor.dbType) == 1)
    }

    processor.pushEvent(DropDatabaseEvent("db1"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.deleteEntityCall.size > 0)
      assert(atlasClient.deleteEntityCall(processor.dbType) == 1)
    }
  }

  test("correctly handle table related events") {
    val processor =
      new SparkCatalogEventProcessor(new FirehoseAtlasClient(atlasClientConf), atlasClientConf)
    processor.startThread()

    var atlasClient: FirehoseAtlasClient = null
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      processor.atlasClient should not be (null)
      atlasClient = processor.atlasClient.asInstanceOf[FirehoseAtlasClient]
    }

    val tableDefinition =
      createTable("db1", "tbl1", new StructType().add("id", LongType), CatalogStorageFormat.empty)
    val isHiveTbl = processor.isHiveTable(tableDefinition)
    SparkUtils.getExternalCatalog().createTable(tableDefinition, ignoreIfExists = true)
    processor.pushEvent(CreateTableEvent("db1", "tbl1"))

    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.createEntityCall(processor.dbType) == 1)
      assert(atlasClient.createEntityCall(processor.tableType(isHiveTbl)) == 1)
      assert(atlasClient.createEntityCall(processor.columnType(isHiveTbl)) == 1)
      assert(atlasClient.createEntityCall(processor.storageFormatType(isHiveTbl)) == 1)
    }

    SparkUtils.getExternalCatalog().renameTable("db1", "tbl1", "tbl2")
    processor.pushEvent(RenameTableEvent("db1", "tbl1", "tbl2"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.updateEntityCall(processor.storageFormatType(isHiveTbl)) == 1)
      assert(atlasClient.updateEntityCall(processor.columnType(isHiveTbl)) == 1)
      assert(atlasClient.updateEntityCall(processor.tableType(isHiveTbl)) == 1)
    }

    processor.pushEvent(DropTablePreEvent("db1", "tbl2"))
    processor.pushEvent(DropTableEvent("db1", "tbl2"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.deleteEntityCall(processor.tableType(isHiveTbl)) == 1)
    }
  }
}

class FirehoseAtlasClient(conf: AtlasClientConf) extends AtlasClient {
  var createEntityCall = new mutable.HashMap[String, Int]
  var updateEntityCall = new mutable.HashMap[String, Int]
  var deleteEntityCall = new mutable.HashMap[String, Int]

  var processedEntity: AtlasEntity = _

  override def createAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = { }

  override def updateAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = { }

  override def getAtlasTypeDefs(searchParams: MultivaluedMapImpl): AtlasTypesDef = {
    new AtlasTypesDef()
  }

  override protected def doCreateEntities(entities: Seq[AtlasEntity]): Unit = {
    entities.foreach { e =>
      createEntityCall(e.getTypeName) =
        createEntityCall.getOrElseUpdate(e.getTypeName, 0) + 1
      processedEntity = e
    }
  }

  override protected def doUpdateEntityWithUniqueAttr(
      entityType: String,
      attribute: String,
      entity: AtlasEntity): Unit = {
    updateEntityCall(entityType) = updateEntityCall.getOrElse(entityType, 0) + 1
    processedEntity = entity
  }

  override protected def doDeleteEntityWithUniqueAttr(
      entityType: String,
      attribute: String): Unit = {
    deleteEntityCall(entityType) = deleteEntityCall.getOrElse(entityType, 0) + 1
  }

  override def getAtlasEntitiesWithUniqueAttribute(
      entityType: String,
      attribute: String): AtlasEntity = {
    ???
  }

  override def getAtlasEntityWithGuid(guid: String): AtlasEntity = {
    ???
  }

  override def deleteAtlasEntitiesWithGuidBulk(guid: Seq[String]): Unit = {
    ???
  }

  override def deleteAtlasEntitiesWithGuid(guid: String): Unit = {
    ???
  }

  override def doSearchByDSL(qualifiedName: String, entityType: String): Seq[AtlasEntityHeader] = {
    ???
  }

  override def putEntityByGuid(atlasEntity: AtlasEntityWithExtInfo): Unit = {
    ???
  }
}
