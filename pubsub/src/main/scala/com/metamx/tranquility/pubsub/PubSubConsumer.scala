package com.metamx.tranquility.pubsub

import java.io.ByteArrayInputStream
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.GZIPInputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}
import com.jayway.jsonpath.{JsonPath, PathNotFoundException}
import com.metamx.common.scala.Logging
import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.pubsub.model.PropertiesBasedPubSubConfig
import com.metamx.tranquility.pubsub.writer.WriterController
import net.minidev.json.JSONArray
import scala.collection.JavaConverters._

import scala.collection.JavaConverters._

class PubSubConsumer(config: PropertiesBasedPubSubConfig,
                     dataSourceConfig: Map[String, DataSourceConfig[PropertiesBasedPubSubConfig]],
                     writerController: WriterController) extends Logging {
  val shutdown = new AtomicBoolean()

  val subscribers: Seq[Subscriber] = getSubscribers(config)

  def start(): Unit = {
    startConsumers()
  }

  def stop(): Unit = {
    if (shutdown.compareAndSet(false, true)) {
      log.info("Shutting down - attempting to flush buffers and commit final offsets")
      writerController.flushAll()
      writerController.stop()

      subscribers.foreach(_.stopAsync())
    }
  }

  private def startConsumers(): Unit = {
    subscribers.foreach(_.startAsync())
  }

  private def getSubscribers(config: PropertiesBasedPubSubConfig): Seq[Subscriber] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    dataSourceConfig.map { conf =>
      val receiver =
        new MessageReceiver {
          override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
            val decompressedData: String = if (config.decompressData) {
              val inputStream = new GZIPInputStream(new ByteArrayInputStream(message.getData.toByteArray))
              scala.io.Source.fromInputStream(inputStream).mkString
            } else {
              new String(message.getData.toByteArray)
            }

            // creates JSON object with pubsub messages attributes and data
            val jsonData = mapper.writeValueAsString(message.getAttributesMap.asScala ++ Map(
              "data" -> mapper.readValue(decompressedData, classOf[Map[String, Any]])))

            conf._2.propertiesBasedConfig.getSplitFields.foreach { case(jsonPath, mapPath) =>
              val json = JsonPath.parse(jsonData)
              try {
                val elements: JSONArray = json.read(jsonPath)
                val elementArray: Array[Array[Any]] = mapper.readValue(elements.toJSONString, classOf[Array[Array[Any]]])

                elementArray.foreach { el =>
                  val newJson = json.put("$", mapPath.toString, el).jsonString()
                  writerController.getWriter(conf._2.propertiesBasedConfig.topic).send(newJson.getBytes)
                }
              } catch {
                case e: PathNotFoundException =>
                  // path not found
              }
            }

            consumer.ack()
          }
        }

      val subscriptionName = ProjectSubscriptionName.of(config.projectId, conf._2.propertiesBasedConfig.subscriptionId)
      Subscriber.newBuilder(subscriptionName, receiver).build()
    }.toSeq
  }
}
