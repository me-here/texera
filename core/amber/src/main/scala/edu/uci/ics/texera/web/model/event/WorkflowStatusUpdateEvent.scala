package edu.uci.ics.texera.web.model.event

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  WorkflowResultUpdate,
  WorkflowStatusUpdate
}
import edu.uci.ics.amber.engine.architecture.principal.{
  OperatorResult,
  OperatorState,
  OperatorStatistics
}
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler

object WebOperatorStatistics {

  def apply(
      operatorID: String,
      operatorStatistics: OperatorStatistics,
      workflowCompiler: WorkflowCompiler
  ): WebOperatorStatistics = {
    WebOperatorStatistics(
      operatorStatistics.operatorState,
      operatorStatistics.aggregatedInputRowCount,
      operatorStatistics.aggregatedOutputRowCount
    )
  }

}

case class IncrementalOutputResult(outputMode: IncrementalOutputMode, result: OperatorResult)

case class WebOperatorStatistics(
    operatorState: OperatorState,
    aggregatedInputRowCount: Long,
    aggregatedOutputRowCount: Long
)

object WebWorkflowStatusUpdateEvent {
  def apply(
      update: WorkflowStatusUpdate,
      workflowCompiler: WorkflowCompiler
  ): WebWorkflowStatusUpdateEvent = {
    WebWorkflowStatusUpdateEvent(
      update.operatorStatistics.map(e =>
        (e._1, WebOperatorStatistics.apply(e._1, e._2, workflowCompiler))
      )
    )
  }
}

case class WebWorkflowStatusUpdateEvent(operatorStatistics: Map[String, WebOperatorStatistics])
    extends TexeraWebSocketEvent

object WebIncrementalOperatorResult {
  def apply(
      operatorID: String,
      opResult: OperatorResult,
      workflowCompiler: WorkflowCompiler
  ): Option[WebIncrementalOperatorResult] = {
    val chartType = WebOperatorResult.getChartType(operatorID, workflowCompiler)
    if (chartType.isEmpty) {
      Option.empty
    } else {
      val webResult =
        WebOperatorResult.fromTuple(operatorID, opResult.result, chartType, opResult.result.size)
      Option(WebIncrementalOperatorResult(opResult.outputMode, webResult))
    }
  }
}
case class WebIncrementalOperatorResult(
    outputMode: IncrementalOutputMode,
    result: WebOperatorResult
)

object WebWorkflowResultUpdateEvent {
  def apply(
      update: WorkflowResultUpdate,
      workflowCompiler: WorkflowCompiler
  ): WebWorkflowResultUpdateEvent = {
    val resultMap = update.operatorResults
      .map(e => (e._1, WebIncrementalOperatorResult.apply(e._1, e._2, workflowCompiler)))
      .filter(e => e._2.nonEmpty)
      .map(e => (e._1, e._2.get))
    WebWorkflowResultUpdateEvent(resultMap)
  }
}
case class WebWorkflowResultUpdateEvent(operatorResults: Map[String, WebIncrementalOperatorResult])
    extends TexeraWebSocketEvent
