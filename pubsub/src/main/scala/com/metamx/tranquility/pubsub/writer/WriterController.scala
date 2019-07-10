package com.metamx.tranquility.pubsub.writer

import com.metamx.common.scala.Logging
import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.finagle.FinagleRegistry
import com.metamx.tranquility.pubsub.model.{MessageCounters, PropertiesBasedPubSubConfig}
import org.apache.curator.framework.CuratorFramework

import scala.collection._
import scala.collection.concurrent.TrieMap

class WriterController(dataSourceConfigs: Map[String, DataSourceConfig[PropertiesBasedPubSubConfig]]) extends Logging {
  val writers: concurrent.Map[String, TranquilityEventWriter] = TrieMap.empty[String, TranquilityEventWriter]
  val dataSourceConfigList: Seq[DataSourceConfig[PropertiesBasedPubSubConfig]] = dataSourceConfigs.values.toSeq.sortBy(_.propertiesBasedConfig.getTopicPatternPriority)
  val curators: concurrent.Map[String, CuratorFramework] = TrieMap.empty[String, CuratorFramework]
  val finagleRegistries: concurrent.Map[String, FinagleRegistry] = TrieMap.empty[String, FinagleRegistry]

  // todo: sort config based on priority?

  def getWriter(topic: String): TranquilityEventWriter = {
    this.synchronized {
      if (!writers.contains(topic)) {
        dataSourceConfigList.foreach { dataSourceConfig =>
          if (dataSourceConfig.propertiesBasedConfig.getTopicPattern == topic) {
            log.info(s"Creating EventWriter for topic $topic using dataSource ${dataSourceConfig.dataSource}")
            writers.put(topic, createWriter(topic, dataSourceConfig))
            return writers(topic)
          }
        }
      }

      writers(topic)
    }
  }

  def createWriter(topic: String, dataSourceConfig: DataSourceConfig[PropertiesBasedPubSubConfig]): TranquilityEventWriter = {
    new TranquilityEventWriter(
      topic,
      dataSourceConfig
    )
  }

  def flushAll(): Map[String, MessageCounters] = {
    writers.mapValues { writer =>
      writer.flush()
      writer.getMessageCounters
    }
  }

  def stop(): Unit = {
    writers.values.foreach(_.stop())
    curators.values.foreach(_.close())
  }
}
