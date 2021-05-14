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

abstract class LogStorage(val id: String) extends Serializable {

  // for persist:
  def write(record:LogRecord)

  def getStepCursor:Long

  // commit all record before last commit
  def commit()

  // for recovery:
  def getLogs: Iterable[LogRecord]

  // delete everything
  def clear(): Unit

  // release the resources
  def release(): Unit

}
