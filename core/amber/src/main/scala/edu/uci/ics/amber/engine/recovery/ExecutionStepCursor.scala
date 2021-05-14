package edu.uci.ics.amber.engine.recovery

class ExecutionStepCursor {
  private var cursor = 0L

  @inline def increment():Unit = {
     cursor+=1
  }

  @inline def getCursor:Long = cursor
}
