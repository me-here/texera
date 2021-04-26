package edu.uci.ics.amber.engine.recovery
import edu.uci.ics.amber.engine.common.ambermessage.{DPCursor, DataBatchSequence, LogRecord, LogWriterPayload, WorkflowControlMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

class EmptyLogStorage(id:String) extends LogStorage(id) {

  override def clear(): Unit = {}

  override def release(): Unit = {}

  override def writeControlLogRecord(record: WorkflowControlMessage): Unit = {}

  override def commit(): Unit = {}

  override def getLogs: Iterable[LogRecord] = Iterable.empty

  override def writeDataLogRecord(from: VirtualIdentity, seq:Long): Unit = {}

  override def writeDPLogRecord(cursor: Long): Unit = {}
}
