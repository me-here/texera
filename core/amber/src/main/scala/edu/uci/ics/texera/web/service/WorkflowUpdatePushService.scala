package edu.uci.ics.texera.web.service

import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent

import java.util.concurrent.locks.ReentrantLock
import javax.websocket.Session
import scala.collection.mutable


object WorkflowUpdatePushService {

  final val objectMapper = Utils.objectMapper

  private val sendLock = new ReentrantLock()
  private val subscribers = new mutable.HashMap[String, Session]()

  def pause():Unit = sendLock.lock()

  def resume():Unit = sendLock.unlock()

  def addSubscriber(session:Session):Unit ={
    subscribers(session.getId) = session
  }

  def removeSubscriber(session:Session):Unit ={
    subscribers.remove(session.getId)
  }

  def sendToSubscribers(message: TexeraWebSocketEvent):Unit ={
    sendLock.lock()
    subscribers.values.foreach{
      session => session.getAsyncRemote.sendText(objectMapper.writeValueAsString(message))
    }
    sendLock.unlock()
  }

  def sendToSubscribers(messages:Array[TexeraWebSocketEvent]): Unit ={
    sendLock.lock()
    messages.foreach {
      message =>
        subscribers.values.foreach {
          session => session.getAsyncRemote.sendText(objectMapper.writeValueAsString(message))
        }
    }
    sendLock.unlock()
  }
}
