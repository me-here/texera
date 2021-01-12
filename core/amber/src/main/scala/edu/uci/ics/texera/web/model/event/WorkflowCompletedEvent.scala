package edu.uci.ics.texera.web.model.event

import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowCompleted
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator

import scala.collection.mutable

<<<<<<< HEAD
object OperatorResult {
  def apply(operatorID: String, table: List[ITuple], workflowCompiler: WorkflowCompiler): OperatorResult = {
    val tableKv = table.map(tuple => tuple.asInstanceOf[Tuple].asKeyValuePairJson())

    // add chartType to result
    val precedentOpID =
      workflowCompiler.workflowInfo.links.find(link => link.destination == operatorID).get.origin
    val precedentOp =
      workflowCompiler.workflowInfo.operators.find(op => op.operatorID == precedentOpID).get
    val chartType = precedentOp match {
      case operator: VisualizationOperator => operator.chartType()
      case _                               => null
    }

//    OperatorResult(operatorID, tableKv, chartType)
  }
}

case class OperatorResult(
    operatorID: String,
    table: List[ObjectNode],
    chartType: String,
    totalRowCount: Int
)

object WorkflowCompletedEvent {
  val defaultPageSize = 10

  // transform results in amber tuple format to the format accepted by frontend
  def apply(
      workflowCompleted: WorkflowCompleted,
      workflowCompiler: WorkflowCompiler
  ): WorkflowCompletedEvent = {
    val resultList = new mutable.MutableList[OperatorResult]
    workflowCompleted.result.foreach(pair => {
//<<<<<<< HEAD
//      resultList += OperatorResult.apply(operatorID = pair._1, pair._2, workflowCompiler)
//=======
//      val operatorID = pair._1
//
//      // add chartType to result
//      val precedentOpID =
//        workflowCompiler.workflowInfo.links.find(link => link.destination == operatorID).get.origin
//      val precedentOp =
//        workflowCompiler.workflowInfo.operators.find(op => op.operatorID == precedentOpID).get
//      val chartType = precedentOp match {
//        case operator: VisualizationOperator => operator.chartType()
//        case _                               => null
//      }
//
//      val table = precedentOp match {
//        case _: VisualizationOperator =>
//          pair._2.map(tuple => tuple.asInstanceOf[Tuple].asKeyValuePairJson())
//        case _ =>
//          pair._2
//            .slice(0, defaultPageSize)
//            .map(tuple => tuple.asInstanceOf[Tuple].asKeyValuePairJson())
//      }
//
//      resultList += OperatorResult(operatorID, table, chartType, pair._2.length)
//>>>>>>> 10dfc4dc95ac9362f3591f1d4e6dcee59b33de28
    })
    WorkflowCompletedEvent(resultList.toList)
  }
}

case class WorkflowCompletedEvent(result: List[OperatorResult]) extends TexeraWebSocketEvent
