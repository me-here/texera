package edu.uci.ics.amber.engine.recovery

import edu.uci.ics.amber.engine.common.ambermessage.{DPCursor, DataBatchSequence, FromSender, LogRecord, LogWriterPayload, WorkflowControlMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

import scala.collection.mutable

object InMemoryLogStorage {

  private lazy val logs = new mutable.HashMap[String, mutable.Queue[LogRecord]]()

  def getLogOf(k: String): mutable.Queue[LogRecord] = {
    if (!logs.contains(k)) {
      logs(k) = new mutable.Queue[LogRecord]()
    }
    logs(k)
  }

  def clearLogOf(k: String): Unit = {
    if (logs.contains(k)) {
      logs.remove(k)
    }
  }

}

class InMemoryLogStorage(logName: String) extends LogStorage(logName) {

  override def writeControlLogRecord(record: WorkflowControlMessage): Unit = {
    InMemoryLogStorage.getLogOf(logName).enqueue(record)
  }

  override def writeDataLogRecord(from:VirtualIdentity): Unit = {
    InMemoryLogStorage.getLogOf(logName).enqueue(FromSender(from))
  }

  override def writeDPLogRecord(idx:Long): Unit = {
    InMemoryLogStorage.getLogOf(logName).enqueue(DPCursor(idx))
  }

  override def commit(): Unit = {}

  override def getLogs: Iterable[LogRecord] = {
    InMemoryLogStorage.getLogOf(logName)
  }

  override def clear(): Unit = {
    InMemoryLogStorage.clearLogOf(logName)
  }

  override def release(): Unit = {}

}
