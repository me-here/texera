package edu.uci.ics.amber.engine.architecture.messaginglayer

import java.util.concurrent.atomic.AtomicLong

import edu.uci.ics.amber.engine.common.ambermessage.{ControlPayload, UpdateStepCursor, WorkflowControlMessage}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{NetworkSenderActorRef, SendRequest, SendRequestOWP}
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.recovery.{ExecutionStepCursor, ParallelLogWriter}

import scala.collection.mutable

/** This class handles the assignment of sequence numbers to controls
  * The internal logic can send control messages to other actor without knowing
  * where the actor is and without determining the sequence number.
  */
class ControlOutputPort(
                         selfID: ActorVirtualIdentity,
                         networkSenderActor: NetworkSenderActorRef,
                         stepCursor: ExecutionStepCursor,
                         logWriter: ParallelLogWriter
) {

  protected val logger: WorkflowLogger = WorkflowLogger("ControlOutputPort")

  private val idToSequenceNums = new mutable.AnyRefMap[ActorVirtualIdentity, AtomicLong]()

  def sendTo(to: ActorVirtualIdentity, payload: ControlPayload): Unit = {
    var receiverId = to
    if (to == ActorVirtualIdentity.Self) {
      // selfID and VirtualIdentity.Self should be one key
      receiverId = selfID
    }
    val seqNum = idToSequenceNums.getOrElseUpdate(receiverId, new AtomicLong()).getAndIncrement()
    val msg = WorkflowControlMessage(selfID, seqNum, payload)
    logWriter.addLogRecord(UpdateStepCursor(stepCursor.getCursor))
    networkSenderActor ! SendRequest(
      to,
      msg,
      stepCursor.getCursor
    )
  }

  def sendToOWP(closure: () => Unit): Unit = {
    logWriter.addLogRecord(UpdateStepCursor(stepCursor.getCursor))
    networkSenderActor ! SendRequestOWP(
      closure,
      stepCursor.getCursor
    )
  }

}
