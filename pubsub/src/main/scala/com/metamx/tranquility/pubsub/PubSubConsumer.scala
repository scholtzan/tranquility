package com.metamx.tranquility.pubsub

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.metamx.common.scala.Logging
import com.metamx.tranquility.pubsub.model.PropertiesBasedPubSubConfig
import com.metamx.tranquility.pubsub.writer.WriterController
import java.util.concurrent.atomic.AtomicBoolean

import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.pubsub.v1.{ProjectSubscriptionName, ProjectTopicName, PubsubMessage}
import com.metamx.tranquility.config.DataSourceConfig

class PubSubConsumer(config: PropertiesBasedPubSubConfig,
                     dataSourceConfig: Map[String, DataSourceConfig[PropertiesBasedPubSubConfig]],
                     writerController: WriterController) extends Logging {
  val commitMillis: Int = config.commitMillis
  val commitLock = new ReentrantReadWriteLock()
  val shutdown = new AtomicBoolean()

  val subscribers: Seq[Subscriber] = getSubscribers(config)

  def start(): Unit = {
    startConsumers()
  }

  def stop(): Unit = {
    if (shutdown.compareAndSet(false, true)) {
      log.info("Shutting down - attempting to flush buffers and commit final offsets")

      commitLock.writeLock().lockInterruptibly()
      writerController.flushAll()
      writerController.stop()

      subscribers.foreach(_.stopAsync())
    }
  }

  private def startConsumers(): Unit = {
    subscribers.foreach(_.startAsync())
  }

  private def getSubscribers(config: PropertiesBasedPubSubConfig): Seq[Subscriber] = {
    dataSourceConfig.map { conf =>
      val receiver =
        new MessageReceiver {
          override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
            commitLock.readLock().lockInterruptibly()
            println("Message: " + message.getData.toString)
            writerController.getWriter(conf._2.propertiesBasedConfig.getTopic).send(message.getData.toByteArray)
            consumer.ack()
            commitLock.readLock().unlock()
          }
        }

      val subscriptionName = ProjectSubscriptionName.of(config.getProjectId, conf._2.propertiesBasedConfig.getSubscriptionId)
      Subscriber.newBuilder(subscriptionName, receiver).build()
    }.toSeq
  }
}
