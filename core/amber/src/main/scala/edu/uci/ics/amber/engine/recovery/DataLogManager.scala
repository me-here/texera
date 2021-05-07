package edu.uci.ics.amber.engine.recovery

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  BatchToTupleConverter,
  NetworkAckManager
}
import edu.uci.ics.amber.engine.common.ambermessage.{
  DataBatchSequence,
  DataPayload,
  FromSender,
  WorkflowDataMessage
}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

import scala.collection.mutable

class DataLogManager(
    logStorage: LogStorage,
    logWriter: ParallelLogWriter,
    networkAckManager: NetworkAckManager,
    handler: (VirtualIdentity, DataPayload) => Unit
) extends RecoveryComponent {

  private val persistedDataOrder: mutable.Queue[VirtualIdentity] =
    logStorage.getLogs
      .collect {
        case msg: FromSender => msg.virtualId
      }
      .to[mutable.Queue]

  private val stashedMessages =
    mutable.HashMap[VirtualIdentity, mutable.Queue[(ActorRef, Long, DataPayload)]]()
  private var enableLogWrite = true

  checkIfCompleted()

  def handleMessage(
      sender: ActorRef,
      id: Long,
      from: VirtualIdentity,
      message: DataPayload
  ): Unit = {
    if (!isRecovering) {
      persistDataSender(sender, id, from, message.size)
      handler(from, message)
    } else {
      if (!stashedMessages.contains(from)) {
        stashedMessages(from) = mutable.Queue[(ActorRef, Long, DataPayload)]()
      }
      stashedMessages(from).enqueue((sender, id, message))
      var persisted = persistedDataOrder.head
      while (
        isRecovering && stashedMessages.contains(persisted) && stashedMessages(persisted).nonEmpty
      ) {
        val (orderedSender, orderedID, orderedMsg) = stashedMessages(persisted).dequeue()
        networkAckManager.advanceSeq(persisted, 1)
        networkAckManager.ackDirectly(orderedSender, orderedID)
        handler(persisted, orderedMsg)
        persistedDataOrder.dequeue()
        if (persistedDataOrder.nonEmpty) {
          persisted = persistedDataOrder.head
        }
        checkIfCompleted()
      }
    }
  }

  def persistDataSender(sender: ActorRef, id: Long, vid: VirtualIdentity, batchSize: Int): Unit = {
    if (enableLogWrite) {
      networkAckManager.enqueueDelayedAck(sender, id)
      logWriter.addLogRecord(DataBatchSequence(vid, batchSize))
    } else {
      networkAckManager.advanceSeq(vid, 1)
      networkAckManager.ackDirectly(sender, id)
    }
  }

  private[this] def checkIfCompleted(): Unit = {
    if (persistedDataOrder.isEmpty && isRecovering) {
      setRecoveryCompleted()
      stashedMessages.foreach {
        case (from, queue) =>
          queue.foreach {
            case (sender, id, msg) =>
              persistDataSender(sender, id, from, msg.size)
              handler(from, msg)
          }
      }
      stashedMessages.clear()
    }
  }

  def disable(): Unit = {
    enableLogWrite = false
  }

}
