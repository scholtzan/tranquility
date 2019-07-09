package com.metamx.tranquility.pubsub

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.metamx.common.scala.Logging
import com.metamx.tranquility.pubsub.model.PropertiesBasedPubSubConfig
import com.metamx.tranquility.pubsub.writer.WriterController
import java.util.concurrent.atomic.AtomicBoolean

import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}
import com.metamx.tranquility.config.DataSourceConfig

class PubSubConsumer(config: PropertiesBasedPubSubConfig,
                     dataSourceConfig: Map[String, DataSourceConfig[PropertiesBasedPubSubConfig]],
                     writerController: WriterController) extends Logging {
  val commitThread: Thread = new Thread(createCommitRunnable())
  val commitMillis: Int = config.commitMillis
  val commitLock = new ReentrantReadWriteLock()
  val shutdown = new AtomicBoolean()

  val subscribers: Seq[Subscriber] = getSubscribers(config)

  def start(): Unit = {
    commitThread.start()
    startConsumers()
  }

  def stop(): Unit = {
    if (shutdown.compareAndSet(false, true)) {
      log.info("Shutting down - attempting to flush buffers and commit final offsets")

      commitLock.writeLock().lockInterruptibly()
      writerController.flushAll()
      writerController.stop()

      subscribers.foreach(_.stopAsync())

      // todo
    }
  }

  def join(): Unit = {
    commitThread.join()
  }

  def createCommitRunnable(): Runnable = {
    new Runnable {
      override def run(): Unit = {
        var lastFlushTime = System.currentTimeMillis()

        while (!Thread.currentThread().isInterrupted) {
          Thread.sleep(math.max(commitMillis - (System.currentTimeMillis() - lastFlushTime), 0))
          commit()
          lastFlushTime = System.currentTimeMillis()
        }
      }
    }
  }

  def commit(): Unit = {
    commitLock.writeLock().lockInterruptibly()
    val flushStartTime = System.currentTimeMillis()
    val messageCounters = writerController.flushAll()

    // todo?

  }

  private def startConsumers(): Unit = {
    subscribers.foreach(_.startAsync())
  }

  private def getSubscribers(config: PropertiesBasedPubSubConfig): Seq[Subscriber] = {
    val subscriptionName = ProjectSubscriptionName.of(config.projectId, config.subscriptionId)

    val receiver =
      new MessageReceiver {
        override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
          commitLock.readLock().lockInterruptibly()
          writerController.getWriter(message.getMessageId).send(message.getData.toByteArray)
          consumer.ack()
          commitLock.readLock().unlock()
        }
      }

    dataSourceConfig.map { conf =>
      Subscriber.newBuilder(conf._2.propertiesBasedConfig.getTopicPattern, receiver).build()
    }.toSeq
  }
}
