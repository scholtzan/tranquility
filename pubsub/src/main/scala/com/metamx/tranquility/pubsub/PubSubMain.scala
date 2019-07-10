package com.metamx.tranquility.pubsub

import java.io.FileInputStream

import com.metamx.common.lifecycle.Lifecycle
import com.metamx.common.scala.lifecycle._
import com.metamx.common.scala.{Abort, Logging}
import com.metamx.common.scala.Predef._
import com.metamx.tranquility.config.{DataSourceConfig, TranquilityConfig}
import com.metamx.tranquility.pubsub.model.PropertiesBasedPubSubConfig
import com.metamx.tranquility.pubsub.writer.WriterController
import com.twitter.app.{App, Flag}

import collection.JavaConverters._

object PubSubMain extends App with Logging {
  private val ConfigResource = "tranquility-pubsub.yaml"

  private val configFile: Flag[String] = flag(
    "configFile",
    s"Path to alternate config file, from working directory (default: $ConfigResource from classpath)"
  )

  def main(): Unit = {
    val configInputStream = configFile.get match {
      case Some(file) =>
        log.info(s"Reading configuration from file[$file].")
        new FileInputStream(file)

      case None =>
        log.info(s"Reading configuration from resource[$ConfigResource].")
        getClass.getClassLoader.getResourceAsStream(ConfigResource) mapNull {
          System.err.println(s"Expected resource $ConfigResource (or provide -configFile <path>)")
          sys.exit(1)
        }
    }

    log.info("Init pubsub")

    val lifecycle = new Lifecycle
    val config = TranquilityConfig.read(configInputStream, classOf[PropertiesBasedPubSubConfig])
    val globalConfig = config.globalConfig

    val dataSourceConfigs: Map[String, DataSourceConfig[PropertiesBasedPubSubConfig]] = config.getDataSources.asScala.map { dataSource =>
      dataSource -> config.getDataSource(dataSource)
    }.toMap

    val writerController = new WriterController(dataSourceConfigs)
    val pubSubConsumer = new PubSubConsumer(globalConfig, dataSourceConfigs, writerController)

    lifecycle onStart {
      log.info("Starting consumers")
      pubSubConsumer.start()
    } onStop {
      log.info("Shutting down")
      pubSubConsumer.stop()
    }

    try {
      lifecycle.start()
    }
    catch {
      case e: Exception =>
        Abort(e)
    }

    lifecycle.join()
    pubSubConsumer.join()
  }
}
