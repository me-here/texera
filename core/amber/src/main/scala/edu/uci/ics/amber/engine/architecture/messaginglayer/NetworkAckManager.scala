package edu.uci.ics.amber.engine.architecture.messaginglayer

import java.util.concurrent.LinkedBlockingQueue

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkAck
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

import scala.collection.mutable

class NetworkAckManager(logEnabled: Boolean) {

  private val ackQueue = new LinkedBlockingQueue[(ActorRef, Long)]()
  private val loggedSeq = mutable.AnyRefMap[VirtualIdentity, Long]().withDefaultValue(0L)

  def enqueueDelayedAck(sender: ActorRef, id: Long): Unit = {
    if (logEnabled) {
      ackQueue.add((sender, id))
    } else {
      ackDirectly(sender, id)
    }
  }

  def enqueuePlaceholderAck(): Unit = {
    if (logEnabled) {
      ackQueue.add((null, 0))
    }
  }
  def ackDuplicated(sender: ActorRef, id: Long, vid: VirtualIdentity, seq: Long): Unit = {
    if (logEnabled) {
      if (loggedSeq(vid) > seq) {
        //duplicated, ack directly
        sender ! NetworkAck(id)
      }
    } else {
      ackDirectly(sender, id)
    }
  }

  @inline def ackDirectly(sender: ActorRef, id: Long): Unit = sender ! NetworkAck(id)

  def advanceSeq(vid: VirtualIdentity, delta: Long): Unit = {
    loggedSeq(vid) += delta
  }

  def releaseAcks(count: Int): Unit = {
    //println(s"release $count acks while we have ${ackQueue.size} acks in queue")
    for (_ <- 0 until count) {
      val item = ackQueue.take()
      if (item._1 != null) {
        item._1 ! NetworkAck(item._2)
      }
    }
  }

}
