package edu.uci.ics.texera.web.model.event

import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowCompleted
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator

import scala.collection.mutable

object WebOperatorResult {
  def getChartType(operatorID: String, workflowCompiler: WorkflowCompiler): Option[String] = {
    if (!WorkflowCompiler.isSink(operatorID, workflowCompiler)) {
      return None
    }

    // add chartType to result
    val upstreamOperators = WorkflowCompiler.getUpstreamOperators(operatorID, workflowCompiler)
    upstreamOperators.headOption match {
      case Some(op) =>
        op match {
          case visOp: VisualizationOperator => Option.apply(visOp.chartType())
          case _                            => Option.empty
        }
      case _ => Option.empty
    }
  }

  def fromTuple(
      operatorID: String,
      table: List[ITuple],
      chartType: Option[String],
      totalRowCount: Int
  ): WebOperatorResult = {
    WebOperatorResult(
      operatorID,
      table.map(t => t.asInstanceOf[Tuple].asKeyValuePairJson()),
      chartType,
      totalRowCount
    )
  }
}

case class WebOperatorResult(
    operatorID: String,
    table: List[ObjectNode],
    chartType: Option[String],
    totalRowCount: Int
)

object WorkflowCompletedEvent {
  val defaultPageSize = 10

  // transform results in amber tuple format to the format accepted by frontend
  def apply(
      workflowCompleted: WorkflowCompleted,
      workflowCompiler: WorkflowCompiler
  ): WorkflowCompletedEvent = {
    val resultList = new mutable.MutableList[WebOperatorResult]
    for ((operatorID, resultTuples) <- workflowCompleted.result) {
      val chartType = WebOperatorResult.getChartType(operatorID, workflowCompiler)

      var table = resultTuples
      // if not visualization result, then only return first page results
      if (chartType.isEmpty) {
        table = resultTuples.slice(0, defaultPageSize)
      }

      resultList += WebOperatorResult.fromTuple(operatorID, table, chartType, resultTuples.length)
    }
    WorkflowCompletedEvent(resultList.toList)
  }
}

case class WorkflowCompletedEvent(result: List[WebOperatorResult]) extends TexeraWebSocketEvent
