package edu.uci.ics.amber.engine.recovery

import edu.uci.ics.amber.engine.architecture.messaginglayer.{NetworkAckManager, NetworkInputPort}
import edu.uci.ics.amber.engine.architecture.worker.DataProcessor
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlLogPayload,
  ControlPayload,
  WorkflowControlMessage
}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

class ControlLogManager(
    storage: LogStorage,
    logWriter: ParallelLogWriter,
    controlInputPort: NetworkInputPort[ControlPayload],
    networkAckManager: NetworkAckManager,
    recoveredControlHandler: (VirtualIdentity, ControlPayload) => Unit
) extends RecoveryComponent {

  // For recovery, only need to replay control messages, and then it's done
  storage.getLogs.foreach {
    case ctrl: ControlLogPayload =>
      controlInputPort.advanceSequenceNumber(ctrl.virtualId)
      networkAckManager.advanceSeq(ctrl.virtualId, 1)
      recoveredControlHandler(ctrl.virtualId, ctrl.payload)
    case other =>
    //skip
  }
  setRecoveryCompleted()

  def persistControlMessage(from: VirtualIdentity, payload: ControlPayload): Unit = {
    logWriter.addLogRecord(ControlLogPayload(from, payload))
  }

}
