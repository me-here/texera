package edu.uci.ics.texera.web

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import rx.lang.scala.{Observer, Subject, Subscription}

class WebsocketOutput extends LazyLogging {
  private val wsOutput = Subject[TexeraWebSocketEvent]
  private var unicastTarget: Observer[TexeraWebSocketEvent] = _

  def withUnicastMode(observer: Observer[TexeraWebSocketEvent])(code: => Unit): Unit = {
    unicastTarget = observer
    code
    unicastTarget = null
  }

  def subscribe(observer: Observer[TexeraWebSocketEvent]): Subscription = {
    wsOutput.subscribe(observer)
  }

  def onNext(event: TexeraWebSocketEvent): Unit = {
    logger.info(event.getClass.getName)
    if (unicastTarget != null) {
      unicastTarget.onNext(event)
    } else {
      wsOutput.onNext(event)
    }
  }

}
