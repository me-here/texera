package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.ActorRef

import scala.collection.mutable

/* The abstracted FIFO/exactly-once logic */
class OrderingEnforcer[T] {

  var current = 0L
  val ofoMap = new mutable.LongMap[(ActorRef, Long, T)]

  def isDuplicated(sequenceNumber: Long): Boolean = {
    if (sequenceNumber == -1) return false
    sequenceNumber < current || ofoMap.contains(sequenceNumber)
  }

  def isAhead(sequenceNumber: Long): Boolean = {
    if (sequenceNumber == -1) return false
    sequenceNumber > current
  }

  def stash(sender: ActorRef, id: Long, sequenceNumber: Long, data: T): Unit = {
    ofoMap(sequenceNumber) = (sender, id, data)
  }

  def enforceFIFO(sender: ActorRef, id: Long, data: T): List[(ActorRef, Long, T)] = {
    val res = mutable.ArrayBuffer[(ActorRef, Long, T)]((sender, id, data))
    current += 1
    while (ofoMap.contains(current)) {
      res.append(ofoMap(current))
      ofoMap.remove(current)
      current += 1
    }
    res.toList
  }
}
