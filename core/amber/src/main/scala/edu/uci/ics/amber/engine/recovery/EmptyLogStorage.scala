package edu.uci.ics.amber.engine.recovery
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlLogPayload,
  DPCursor,
  FromSender,
  LogRecord,
  LogWriterPayload,
  WorkflowControlMessage
}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

class EmptyLogStorage(id: String) extends LogStorage(id) {

  override def clear(): Unit = {}

  override def release(): Unit = {}

  override def write(record: LogRecord): Unit = {}

  override def commit(): Unit = {}

  override def getLogs: Iterable[LogRecord] = Iterable.empty

  override def getStepCursor:Long = 0L
}
