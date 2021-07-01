package edu.uci.ics.texera.web.model.event

import com.fasterxml.jackson.databind.node.ObjectNode

case class PaginatedResultEvent(requestID: String, pageIndex: Int, table: List[ObjectNode])
    extends TexeraWebSocketEvent
