package edu.uci.ics.amber.engine.recovery

import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkInputPort
import edu.uci.ics.amber.engine.common.ambermessage.{ControlPayload, WorkflowControlMessage}

class ControlLogManager(
                         storage: LogStorage,
    logWriter: ParallelLogWriter,
    controlInputPort: NetworkInputPort[ControlPayload]
)extends RecoveryComponent {

  // For recovery, only need to replay control messages, and then it's done
  storage.getLogs.foreach {
    case msg:WorkflowControlMessage =>
      controlInputPort.handleAfterFIFO(msg.from, msg.sequenceNumber, msg.payload)
    case other =>
      //skip
  }
  setRecoveryCompleted()

  def persistControlMessage(msg: WorkflowControlMessage): Unit = {
    logWriter.addLogRecord(msg)
  }

}
