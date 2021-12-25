package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState

case class WorkflowStateEvent(state: WorkflowAggregatedState) extends TexeraWebSocketEvent
