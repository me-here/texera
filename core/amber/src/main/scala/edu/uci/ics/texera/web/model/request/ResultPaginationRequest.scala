package edu.uci.ics.texera.web.model.request

case class ResultPaginationRequest(operatorID: String, pageIndex: Int, pageSize: Int) extends TexeraWebSocketRequest
