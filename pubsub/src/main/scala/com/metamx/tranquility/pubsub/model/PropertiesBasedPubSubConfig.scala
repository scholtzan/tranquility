package com.metamx.tranquility.pubsub.model

import java.util

import scala.collection.JavaConverters._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.metamx.common.scala.untyped.Dict
import com.metamx.tranquility.config.PropertiesBasedConfig
import com.fasterxml.jackson.databind.ObjectMapper
import org.skife.config.Config
import scala.collection.immutable.HashMap

abstract class PropertiesBasedPubSubConfig extends PropertiesBasedConfig () {
  @Config(Array("pubsub.topic"))
  def topic: String = ""

  @Config(Array("useTopicAsDataSource"))
  def useTopicAsDataSource = false

  @Config(Array("pubsub.projectId"))
  def projectId: String = ""

  @Config(Array("pubsub.subscriptionId"))
  def subscriptionId: String = ""

  // decompresses the gzipped data field of pubsub messages if true
  @Config(Array("pubsub.decompressData"))
  def decompressData = true

  // split array into separate events
  @Config(Array("pubsub.splitFields"))
  def splitFields: String = ""

  def getSplitFields: HashMap[String, String] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(splitFields, classOf[HashMap[String, String]])
  }
}
