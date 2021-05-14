package edu.uci.ics.amber.engine.recovery

import edu.uci.ics.amber.engine.common.ambermessage.{DPCursor, LogRecord, UpdateStepCursor}

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

  override def write(record:LogRecord): Unit = {
    InMemoryLogStorage.getLogOf(logName).enqueue(record)
  }

  override def commit(): Unit = {}

  override def getLogs: Iterable[LogRecord] = {
    InMemoryLogStorage.getLogOf(logName)
  }

  override def getStepCursor:Long = getLogs.collect{
    case DPCursor(idx) => idx
    case UpdateStepCursor(cursor) => cursor
  }.last

  override def clear(): Unit = {
    InMemoryLogStorage.clearLogOf(logName)
  }

  override def release(): Unit = {}

}
