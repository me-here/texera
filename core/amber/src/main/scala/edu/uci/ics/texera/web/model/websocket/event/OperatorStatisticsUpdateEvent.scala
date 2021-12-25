package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.principal.{OperatorState, OperatorStatistics}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.web.workflowruntimestate.OperatorRuntimeStats
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler

case class OperatorStatisticsUpdateEvent(operatorStatistics: Map[String, OperatorRuntimeStats])
    extends TexeraWebSocketEvent
