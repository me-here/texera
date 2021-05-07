package edu.uci.ics.amber.engine.recovery
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlLogPayload,
  DPCursor,
  DataBatchSequence,
  LogRecord,
  LogWriterPayload,
  WorkflowControlMessage
}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

class EmptyLogStorage(id: String) extends LogStorage(id) {

  override def clear(): Unit = {}

  override def release(): Unit = {}

  override def writeControlLogRecord(record: ControlLogPayload): Unit = {}

  override def commit(): Unit = {}

  override def getLogs: Iterable[LogRecord] = Iterable.empty

  override def writeDataLogRecord(from: VirtualIdentity): Unit = {}

  override def writeDPLogRecord(cursor: Long): Unit = {}
}
