package com.metamx.tranquility.pubsub.writer

import com.metamx.common.scala.Logging
import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.pubsub.model.PropertiesBasedPubSubConfig

class TranquilityEventWriter(topic: String, dataSourceConfig: DataSourceConfig[PropertiesBasedPubSubConfig]) extends Logging {

  def send(message: Seq[Byte]): Unit = {

  }
}
