package edu.uci.ics.texera.web

import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import scalapb.GeneratedMessage

trait SyncState[T <: GeneratedMessage] {
  private var state:T = new T()

  def modifyState(func:T => T):Unit = {
    synchronized{
      state = func(state)
    }
  }

  def computeDiff(oldState:T, newState:T):Array[TexeraWebSocketEvent]

  def computeSnapshot:Array[TexeraWebSocketEvent]

}
