package edu.uci.ics.amber.engine.recovery

import edu.uci.ics.amber.engine.architecture.messaginglayer.BatchToTupleConverter
import edu.uci.ics.amber.engine.common.ambermessage.{DataBatchSequence, DataPayload, FromSender, WorkflowDataMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

import scala.collection.mutable

object DataLogManager {
}

class DataLogManager(logStorage: LogStorage, logWriter: ParallelLogWriter, batchToTupleConverter: BatchToTupleConverter) extends RecoveryComponent {

  private val persistedDataOrder:mutable.Queue[FromSender] =
    logStorage
      .getLogs
      .collect {
        case msg: FromSender => msg
      }.to[mutable.Queue]

  private val stashedMessages = mutable.HashMap[FromSender, WorkflowDataMessage]()
  private var nextMessage:WorkflowDataMessage = _
  private val remainingMessages = mutable.Queue[WorkflowDataMessage]()

  checkIfCompleted()

  def feedInMessage(message:WorkflowDataMessage): Unit ={
    val from = FromSender(message.from, message.sequenceNumber)
    stashedMessages(from) = message
  }

  def hasNext:Boolean= {
    checkIfCompleted()
    if(isRecovering){
      val from = persistedDataOrder.head
      if (stashedMessages.contains(from)) {
        nextMessage = stashedMessages(from)
        persistedDataOrder.dequeue()
      }
    }
    nextMessage != null || remainingMessages.nonEmpty
  }

  def next():WorkflowDataMessage ={
    if(nextMessage != null){
      val ret = nextMessage
      nextMessage = null
      ret
    }else{
      remainingMessages.dequeue()
    }
  }

   def persistDataSender(vid: VirtualIdentity, batchSize: Int, seq:Long): Unit = {
     if(batchToTupleConverter.getNumOfInputWorkers > 1){
       logWriter.addLogRecord(DataBatchSequence(vid, batchSize, seq))
     }
  }

  private[this] def checkIfCompleted(): Unit = {
    if (persistedDataOrder.isEmpty && isRecovering) {
      stashedMessages.values.foreach(msg => remainingMessages.enqueue(msg))
      setRecoveryCompleted()
    }
  }

}
