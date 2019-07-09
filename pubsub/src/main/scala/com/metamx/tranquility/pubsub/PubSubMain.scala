package com.metamx.tranquility.pubsub

import java.io.FileInputStream

import com.metamx.common.lifecycle.Lifecycle
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.tranquility.config.TranquilityConfig
import com.metamx.tranquility.pubsub.model.PropertiesBasedPubSubConfig
import com.twitter.app.{App, Flag}

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

    val lifecycle = new Lifecycle
    val config = TranquilityConfig.read(configInputStream, classOf[PropertiesBasedPubSubConfig])

  }
}
