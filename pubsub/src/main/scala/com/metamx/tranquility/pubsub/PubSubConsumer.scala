package com.metamx.tranquility.pubsub

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.metamx.common.scala.Logging
import com.metamx.tranquility.pubsub.model.PropertiesBasedPubSubConfig
import com.metamx.tranquility.pubsub.writer.WriterController
import java.util.concurrent.atomic.AtomicBoolean

class PubSubConsumer(config: PropertiesBasedPubSubConfig, writerController: WriterController) extends Logging {
  val commitThread: Thread = new Thread(createCommitRunnable())
  val commitMillis: Int = config.commitMillis
  val commitLock = new ReentrantReadWriteLock()
  val shutdown = new AtomicBoolean()

  def start(): Unit = {
    commitThread.start()
    startConsumers()
  }

  def commit(): Unit = {

  }

  def stop(): Unit = {
    if (shutdown.compareAndSet(false, true)) {
      log.info("Shutting down - attempting to flush buffers and commit final offsets")

      // todo
      try {
        commitLock.writeLock().lockInterruptibly()

        try {
          writerController.flushAll()
        }
      }
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

  private def startConsumers(): Unit = {

  }
}
