package com.metamx.tranquility.pubsub.model

import com.ibm.icu.impl.duration.Period
import org.skife.config.Config
import com.metamx.tranquility.config.PropertiesBasedConfig

abstract class PropertiesBasedPubSubConfig extends PropertiesBasedConfig (Set("http.port", "http.threads", "http.idleTimeout", "pubsub.projectId")) {
  @Config(Array("http.port"))
  def httpPort: Int = 8085

  @Config(Array("http.port.enable"))
  def httpPortEnable: Boolean = true

  @Config(Array("https.port"))
  def httpsPort: Int = 8400

  @Config(Array("https.port.enable"))
  def httpsPortEnable: Boolean = false

  @Config(Array("http.threads"))
  def httpThreads: Int = 40

  @Config(Array("topicPattern"))
  def getTopicPattern: String = ""

  @Config(Array("topicPattern.priority"))
  def getTopicPatternPriority: Int = 1

  @Config(Array("useTopicAsDataSource"))
  def useTopicAsDataSource = false

  @Config(Array("commit.periodMills"))
  def commitMillis: Int = 1000

  @Config(Array("pubsub.projectId"))
  def projectId: String = ""

  @Config(Array("subscriptionId"))
  def subscriptionId: String = ""
}
