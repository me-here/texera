package edu.uci.ics.amber.engine.recovery

import edu.uci.ics.amber.engine.common.ambermessage.{DPCursor, UpdateStepCursor}

import scala.collection.mutable

class DPLogManager(logStorage: LogStorage, logWriter: ParallelLogWriter) extends RecoveryComponent {
  private val targetStepCursor = logStorage.getStepCursor
  private val correlatedSeq = logStorage.getLogs
    .collect {
      case DPCursor(idx) => idx
    }
    .to[mutable.Queue]

  checkIfCompleted(0L)

  def persistCurrentDataCursor(cur: Long): Unit = {
    logWriter.addLogRecord(DPCursor(cur))
  }

  def isCurrentCorrelated(cur: Long): Boolean = {
    checkIfCompleted(cur)
    correlatedSeq.nonEmpty && correlatedSeq.head == cur
  }

  def advanceCursor(): Unit = {
    correlatedSeq.dequeue()
  }

  @inline
  private[this] def checkIfCompleted(stepCursor:Long): Unit = {
    if (correlatedSeq.isEmpty && isRecovering && stepCursor == targetStepCursor) {
      setRecoveryCompleted()
    }
  }
}
