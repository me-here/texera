package edu.uci.ics.texera.web.model.websocket.event

case class WorkflowStateEvent(state: ExecutionStatusEnum) extends TexeraWebSocketEvent
