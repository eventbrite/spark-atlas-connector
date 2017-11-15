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

import scala.collection.mutable

import org.apache.spark.SparkConf

import com.hortonworks.spark.atlas.AtlasClientConf.ConfigEntry

class AtlasClientConf(loadFromSysProps: Boolean) {

  def this() = this(loadFromSysProps = true)

  private val configMap = new mutable.HashMap[String, String]()

  if (loadFromSysProps) {
    sys.props.foreach { case (k, v) =>
      if (k.startsWith("spark.atlas")) {
        configMap.put(k.stripPrefix("spark."), v)
      }
    }
  }

  def set(key: String, value: String): AtlasClientConf = {
    configMap.put(key, value)
    this
  }

  def get(key: String, defaultValue: String): String = {
    configMap.getOrElse(key, defaultValue)
  }

  def getOption(key: String): Option[String] = {
    configMap.get(key)
  }

  def get(t: ConfigEntry): String = {
    configMap.get(t.key).getOrElse(t.defaultValue)
  }

  def setAll(confs: Iterable[(String, String)]): AtlasClientConf = {
    confs.foreach { case (k, v) =>
      configMap.put(k.stripPrefix("spark."), v)
    }

    this
  }
}

object AtlasClientConf {
  case class ConfigEntry(key: String, defaultValue: String)

  val ATLAS_REST_ENDPOINT = ConfigEntry("atlas.rest.address", "localhost:21000")

  val BLOCKING_QUEUE_CAPACITY = ConfigEntry("atlas.blockQueue.size", "10000")
  val BLOCKING_QUEUE_PUT_TIMEOUT = ConfigEntry("atlas.blockQueue.putTimeout.ms", "3000")

  val CLIENT_TYPE = ConfigEntry("atlas.client.type", "rest")
  val CLIENT_USERNAME = ConfigEntry("atlas.client.username", "admin")
  val CLIENT_PASSWORD = ConfigEntry("atlas.client.password", "admin")
  val CLIENT_NUM_RETRIES = ConfigEntry("atlas.client.numRetries", "3")

  val CHECK_MODEL_IN_START = ConfigEntry("atlas.client.checkModelInStart", "true")

  def fromSparkConf(conf: SparkConf): AtlasClientConf = {
    new AtlasClientConf(false).setAll(conf.getAll.filter(_._1.startsWith("spark.atlas")))
  }
}