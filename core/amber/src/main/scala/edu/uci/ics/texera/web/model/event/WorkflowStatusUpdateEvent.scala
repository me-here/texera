package edu.uci.ics.texera.web.model.event

import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import edu.uci.ics.amber.engine.architecture.principal.PrincipalState.PrincipalState
import edu.uci.ics.amber.engine.architecture.principal.{PrincipalStateType, PrincipalStatistics}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler

object OperatorStatistics {
  def apply(
      operatorID: String,
      principalStatistics: PrincipalStatistics,
      dirtyPageIndices: Map[String, List[Int]],
      workflowCompiler: WorkflowCompiler
  ): OperatorStatistics = {
    OperatorStatistics(
      principalStatistics.operatorState,
      principalStatistics.aggregatedInputRowCount,
      principalStatistics.aggregatedOutputRowCount,
      principalStatistics.aggregatedOutputResults
        .map(r => {
          val chartType = OperatorResult.getChartType(operatorID, workflowCompiler)
          val resultData = r.map(t => t.asInstanceOf[Tuple].asKeyValuePairJson())

          chartType match {
            case Some(_) =>
              OperatorResult.apply(
                operatorID,
                resultData,
                chartType,
                r.size
              )
            case None =>
              OperatorResult.apply(
                operatorID,
                List.empty,
                chartType,
                r.size
              )
          }
        }),
      dirtyPageIndices.get(operatorID)
    )
  }
}
case class OperatorStatistics(
    @JsonScalaEnumeration(classOf[PrincipalStateType]) operatorState: PrincipalState,
    aggregatedInputRowCount: Long,
    aggregatedOutputRowCount: Long,
    aggregatedOutputResults: Option[OperatorResult], // in case of a sink operator
    aggregatedOutputResultDirtyPageIndices: Option[List[Int]]
)

object WorkflowStatusUpdateEvent {
  def apply(
      principalStatistics: Map[String, PrincipalStatistics],
      sinkOpDirtyPageIndices: Map[String, List[Int]],
      workflowCompiler: WorkflowCompiler
  ): WorkflowStatusUpdateEvent = {
    WorkflowStatusUpdateEvent(
      principalStatistics.map(e =>
        (e._1, OperatorStatistics.apply(e._1, e._2, sinkOpDirtyPageIndices, workflowCompiler))
      )
    )
  }
}

case class WorkflowStatusUpdateEvent(operatorStatistics: Map[String, OperatorStatistics])
    extends TexeraWebSocketEvent
