package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkAck
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

import scala.collection.mutable

class NetworkInputPort[T](
    val logger: WorkflowLogger,
    val networkAckManager: NetworkAckManager,
    val handler: (ActorRef, Long, VirtualIdentity, T) => Unit
) {

  private val idToOrderingEnforcers =
    new mutable.AnyRefMap[VirtualIdentity, OrderingEnforcer[T]]()

  def handleMessage(
      sender: ActorRef,
      id: Long,
      from: VirtualIdentity,
      sequenceNumber: Long,
      payload: T
  ): Unit = {
    val entry = idToOrderingEnforcers.getOrElseUpdate(from, new OrderingEnforcer[T]())
    if (entry.isDuplicated(sequenceNumber)) {
      networkAckManager.ackDuplicated(sender, id, from, sequenceNumber)
      //logger.logInfo(s"receive duplicated: ${payload} from ${from} with seq = $sequenceNumber")
    } else if (entry.isAhead(sequenceNumber)) {
      //logger.logInfo(s"receive ahead: ${payload} from ${from} with seq = $sequenceNumber")
      entry.stash(sender, id, sequenceNumber, payload)
    } else {
      //logger.logInfo(s"receive $payload from $from and FIFO seq = ${entry.current}")
      entry.enforceFIFO(sender, id, payload).foreach(v => handler.apply(v._1, v._2, from, v._3))
    }
  }

  def advanceSequenceNumber(id: VirtualIdentity): Unit = {
    if (!idToOrderingEnforcers.contains(id)) {
      idToOrderingEnforcers(id) = new OrderingEnforcer[T]()
    }
    idToOrderingEnforcers(id).current += 1
  }

}
