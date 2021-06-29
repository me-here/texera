package edu.uci.ics.texera.web.model.event

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.principal.{OperatorState, OperatorStatistics}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler

object WebWorkflowStatusUpdateEvent {
  def apply(update: WorkflowStatusUpdate): WebWorkflowStatusUpdateEvent = {
    WebWorkflowStatusUpdateEvent(update.operatorStatistics)
  }

  def main(args: Array[String]): Unit = {
    var a: Either[WebWorkflowResultUpdateEvent, WebWorkflowResultIndexUpdateEvent] = null
    var b: Either[WebWorkflowResultUpdateEvent, WebWorkflowResultIndexUpdateEvent] = null

    a = Left(WebWorkflowResultUpdateEvent(1))
    b = Right(WebWorkflowResultIndexUpdateEvent("test"))

    System.out.println(objectMapper.writeValueAsString(a))
    System.out.println(objectMapper.writeValueAsString(b))

  }
}

case class WebWorkflowStatusUpdateEvent(operatorStatistics: Map[String, OperatorStatistics])
    extends TexeraWebSocketEvent

object WebWorkflowResultUpdateEvent {}

case class WebWorkflowResultUpdateEvent(a: Int)

object WebWorkflowResultIndexUpdateEvent {}

case class WebWorkflowResultIndexUpdateEvent(b: String)

case class WebIncrementalOperatorResult(
    outputMode: IncrementalOutputMode,
    result: WebOperatorResult
)

//case class WebWorkflowResultUpdateEvent(operatorResults: Map[String, WebIncrementalOperatorResult])
//    extends TexeraWebSocketEvent
