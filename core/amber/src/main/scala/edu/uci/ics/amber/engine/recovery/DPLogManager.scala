package edu.uci.ics.amber.engine.recovery

import edu.uci.ics.amber.engine.common.ambermessage.DPCursor

import scala.collection.mutable

class DPLogManager(logStorage: LogStorage, logWriter: ParallelLogWriter) extends RecoveryComponent {
  private val correlatedSeq = logStorage.getLogs
    .collect {
      case DPCursor(idx) => idx
    }
    .to[mutable.Queue]

  checkIfCompleted()

  def persistCurrentDataCursor(cur: Long): Unit = {
    logWriter.addLogRecord(DPCursor(cur))
  }

  def isCurrentCorrelated(cur: Long): Boolean = {
    correlatedSeq.nonEmpty && correlatedSeq.head == cur
  }

  def advanceCursor(): Unit = {
    correlatedSeq.dequeue()
    checkIfCompleted()
  }

  @inline
  private[this] def checkIfCompleted(): Unit = {
    if (correlatedSeq.isEmpty && isRecovering) {
      setRecoveryCompleted()
    }
  }
}
