package edu.uci.ics.texera.web.model.event

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.principal.{OperatorState, OperatorStatistics}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler

object WorkflowStatusUpdateEvent {
  def apply(update: WorkflowStatusUpdate): WorkflowStatusUpdateEvent = {
    WorkflowStatusUpdateEvent(update.operatorStatistics)
  }
}

case class WorkflowStatusUpdateEvent(operatorStatistics: Map[String, OperatorStatistics])
    extends TexeraWebSocketEvent
