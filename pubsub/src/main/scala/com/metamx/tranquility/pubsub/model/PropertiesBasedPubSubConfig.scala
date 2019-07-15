package com.metamx.tranquility.pubsub.model

import com.metamx.tranquility.config.PropertiesBasedConfig
import org.skife.config.Config

abstract class PropertiesBasedPubSubConfig extends PropertiesBasedConfig () {
  @Config(Array("pubsub.topic"))
  def getTopic: String = ""

  @Config(Array("useTopicAsDataSource"))
  def useTopicAsDataSource = false

  @Config(Array("commit.periodMills"))
  def commitMillis: Int = 1000

  @Config(Array("pubsub.projectId"))
  def getProjectId: String = ""

  @Config(Array("pubsub.subscriptionId"))
  def getSubscriptionId: String = ""
}
