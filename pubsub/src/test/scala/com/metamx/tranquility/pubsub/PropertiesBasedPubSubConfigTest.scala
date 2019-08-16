package com.metamx.tranquility.pubsub

import com.metamx.tranquility.config.TranquilityConfig
import com.metamx.tranquility.pubsub.model.PropertiesBasedPubSubConfig
import org.scalatest.{FunSuite, ShouldMatchers}

import scala.collection.immutable.HashMap

class PropertiesBasedPubSubConfigTest extends FunSuite with ShouldMatchers {
  test("readJsonConfig") {
    val config = TranquilityConfig.read(
      getClass.getClassLoader.getResourceAsStream("tranquility-pubsub.json"),
      classOf[PropertiesBasedPubSubConfig]
    )

    config.globalConfig.projectId should be("test-project")

    config.dataSourceConfigs.keySet should be(Set("test"))

    val testConfig = config.dataSourceConfigs("test")
    testConfig.propertiesBasedConfig.zookeeperConnect should be("1.1.1.1")
    testConfig.propertiesBasedConfig.taskPartitions should be(2)
    testConfig.propertiesBasedConfig.topic should be("test-topic")
    testConfig.propertiesBasedConfig.useTopicAsDataSource should be(false)
    testConfig.propertiesBasedConfig.subscriptionId should be("test-subscription")
    testConfig.propertiesBasedConfig.decompressData should be(true)
    testConfig.propertiesBasedConfig.splitFields should be("{\"$.test.path\":\"foo\"}")
    testConfig.propertiesBasedConfig.getSplitFields should be(HashMap("$.test.path" -> "foo"))
  }
}
