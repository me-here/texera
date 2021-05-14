package edu.uci.ics.amber.engine.common.ambermessage

import akka.actor.Address
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, VirtualIdentity}

sealed trait WorkflowMessage extends Serializable

sealed trait LogWriterPayload extends Serializable

trait WorkflowFIFOMessage extends WorkflowMessage {
  val from: VirtualIdentity
  val sequenceNumber: Long
}

case class WorkflowControlMessage(
    from: VirtualIdentity,
    sequenceNumber: Long,
    payload: ControlPayload
) extends WorkflowFIFOMessage

case class WorkflowDataMessage(
    from: VirtualIdentity,
    sequenceNumber: Long,
    payload: DataPayload
) extends WorkflowFIFOMessage

sealed trait RecoveryMessage extends WorkflowMessage
final case class TriggerRecovery(nodeAddr: Address) extends RecoveryMessage
final case class RecoveryCompleted(id: ActorVirtualIdentity) extends RecoveryMessage
final case class TriggerRecoveryOnWorker(id: ActorVirtualIdentity) extends RecoveryMessage

sealed trait LogRecord

case class FromSender(virtualId: VirtualIdentity) extends LogWriterPayload with LogRecord

case class UpdateStepCursor(cursor:Long) extends LogWriterPayload with LogRecord

case class ControlLogPayload(virtualId: VirtualIdentity, payload: ControlPayload)
    extends LogWriterPayload
    with LogRecord
case class DPCursor(idx: Long) extends LogWriterPayload with LogRecord
case object ShutdownWriter extends LogWriterPayload
