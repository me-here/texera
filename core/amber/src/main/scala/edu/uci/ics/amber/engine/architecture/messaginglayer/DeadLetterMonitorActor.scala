package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.{Actor, DeadLetter}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  MessageBecomesDeadLetter,
  NetworkMessage
}

class DeadLetterMonitorActor extends Actor {
  override def receive: Receive = {
    case d: DeadLetter =>
      d.message match {
        case networkMessage: NetworkMessage =>
          // d.sender is the NetworkSenderActor
          // println(s"message: ${networkMessage.internalMessage} failed")
          d.sender ! MessageBecomesDeadLetter(networkMessage, d.recipient)
        case other =>
        // skip for now
      }
    case _ =>
  }
}
