package com.metamx.tranquility.pubsub.model

case class MessageCounters(
   receivedCount: Long,
   sentCount: Long,
   droppedCount: Long,
   unparseableCount: Long
)
