package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent

case class BreakpointTriggeredEvent(
    report: Iterable[BreakpointEvent],
    operatorID: String
) extends TexeraWebSocketEvent
