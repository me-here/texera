package edu.uci.ics.amber.engine.recovery

import edu.uci.ics.amber.engine.common.ambermessage.{DataBatchSequence, DataPayload, FromSender}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

import scala.collection.mutable

object DataLogManager {
}

class DataLogManager(logStorage: LogStorage, logWriter: ParallelLogWriter) extends RecoveryComponent {

  private val persistedDataOrder:mutable.Queue[VirtualIdentity] =
    logStorage
      .getLogs
      .collect {
        case msg: FromSender =>
          msg.virtualId
      }.to[mutable.Queue]

  private val stashedMessages = mutable.HashMap[VirtualIdentity, mutable.Queue[DataPayload]]()
  private var nextPayload:(VirtualIdentity, DataPayload) = _
  private val remainingMessages = mutable.Queue[(VirtualIdentity, DataPayload)]()

  checkIfCompleted()

  def feedInMessage(from: VirtualIdentity, message: DataPayload): Unit ={
    if (stashedMessages.contains(from)) {
      stashedMessages(from).enqueue(message)
    } else {
      stashedMessages(from) = mutable.Queue[DataPayload](message)
    }
  }

  def hasNext:Boolean= {
    checkIfCompleted()
    if(isRecovering){
      val vid = persistedDataOrder.head
      if (stashedMessages.contains(vid) && stashedMessages(vid).nonEmpty) {
        val payload = stashedMessages(vid).dequeue()
        nextPayload = (vid, payload)
        persistedDataOrder.dequeue()
      }
    }
    nextPayload != null || remainingMessages.nonEmpty
  }

  def next():(VirtualIdentity, DataPayload)={
    if(nextPayload != null){
      val ret = nextPayload
      nextPayload = null
      ret
    }else{
      remainingMessages.dequeue()
    }
  }

   def persistDataSender(vid: VirtualIdentity, batchSize: Int): Unit = {
    logWriter.addLogRecord(DataBatchSequence(vid, batchSize))
  }

  private[this] def checkIfCompleted(): Unit = {
    if (persistedDataOrder.isEmpty && isRecovering) {
      stashedMessages.foreach{
        case(vid, queue) =>
          while(queue.nonEmpty){
            remainingMessages.enqueue((vid, queue.dequeue()))
        }
      }
      setRecoveryCompleted()
    }
  }

}
