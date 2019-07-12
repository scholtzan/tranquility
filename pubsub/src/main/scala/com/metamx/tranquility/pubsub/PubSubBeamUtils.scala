package com.metamx.tranquility.pubsub

import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.druid.{DruidBeams, DruidLocation}
import com.metamx.tranquility.pubsub.model.PropertiesBasedPubSubConfig
import com.metamx.tranquility.tranquilizer.Tranquilizer

import scala.reflect.runtime.universe.typeTag

object PubSubBeamUtils {
  def createTranquilizer(
    topic: String,
    config: DataSourceConfig[PropertiesBasedPubSubConfig]
  ): Tranquilizer[Array[Byte]] = {
    DruidBeams.fromConfig(config, typeTag[Array[Byte]]).location(
      DruidLocation.create(
        config.propertiesBasedConfig.druidIndexingServiceName,
        if (config.propertiesBasedConfig.useTopicAsDataSource) topic else config.dataSource
      )
    ).buildTranquilizer(config.tranquilizerBuilder())
  }
}
