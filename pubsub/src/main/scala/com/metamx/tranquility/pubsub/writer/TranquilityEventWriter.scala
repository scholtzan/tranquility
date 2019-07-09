package com.metamx.tranquility.pubsub.writer

import java.util.concurrent.atomic.AtomicLong

import com.metamx.common.scala.Logging
import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.pubsub.PubSubBeamUtils
import com.metamx.tranquility.pubsub.model.{MessageCounters, PropertiesBasedPubSubConfig}
import com.metamx.tranquility.tranquilizer.{MessageDroppedException, Tranquilizer}
import com.twitter.util.FutureEventListener

import scala.reflect.macros.ParseException

class TranquilityEventWriter(topic: String, dataSourceConfig: DataSourceConfig[PropertiesBasedPubSubConfig]) extends Logging {
  val receivedCounter = new AtomicLong()
  val sentCounter = new AtomicLong()
  val droppedCounter = new AtomicLong()
  val unparseableCounter = new AtomicLong()

  val tranquilizer: Tranquilizer[Array[Byte]] = PubSubBeamUtils.createTranquilizer(topic, dataSourceConfig)
  tranquilizer.start()

  def send(message: Array[Byte]): Unit = {
    receivedCounter.incrementAndGet()

    tranquilizer.send(message).addEventListener {
      new FutureEventListener[Unit] {
        override def onSuccess(value: Unit): Unit = {
          sentCounter.incrementAndGet()
        }

        override def onFailure(cause: Throwable): Unit = {
          cause match {
            case _: MessageDroppedException =>
              droppedCounter.incrementAndGet()
              log.error("Dropped message: " + cause)
            case _: ParseException =>
              unparseableCounter.incrementAndGet()
              log.error("Unparseable message: " + cause)
          }
        }
      }
    }
  }

  def flush(): Unit = {
    tranquilizer.flush()
  }

  def stop(): Unit = {
    tranquilizer.stop()
  }

  def getMessageCounters: MessageCounters = {
    MessageCounters(
      receivedCounter.get(),
      sentCounter.get(),
      droppedCounter.get(),
      unparseableCounter.get()
    )
  }
}
