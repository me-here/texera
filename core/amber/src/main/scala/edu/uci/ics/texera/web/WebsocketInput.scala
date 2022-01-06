package edu.uci.ics.texera.web

import edu.uci.ics.texera.web.model.websocket.request.TexeraWebSocketRequest
import org.jooq.types.UInteger
import rx.lang.scala.{Subject, Subscription}

import scala.reflect.{ClassTag, classTag}

class WebsocketInput(errorHandler: Throwable => Unit) {
  private val wsInput = Subject[(TexeraWebSocketRequest, Option[UInteger])]
  def subscribe[T <: TexeraWebSocketRequest: ClassTag](
      callback: (T, Option[UInteger]) => Unit
  ): Subscription = {
    wsInput.subscribe(evt => {
      evt._1 match {
        case req: T if classTag[T].runtimeClass.isInstance(req) =>
          try {
            callback(req, evt._2)
          } catch {
            case throwable: Throwable =>
              errorHandler(throwable)
          }
        case _ =>
      }
    })
  }

  def onNext(req: TexeraWebSocketRequest, uidOpt: Option[UInteger]): Unit = {
    wsInput.onNext((req, uidOpt))
  }

}
