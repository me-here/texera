package edu.uci.ics.texera.web.resource

import com.fasterxml.jackson.annotation.{JsonTypeInfo, JsonTypeName}
import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowResultUpdate
import edu.uci.ics.amber.engine.architecture.principal.OperatorResult
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.web.model.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.resource.OperatorResultService.{
  PaginationMode,
  SetDeltaMode,
  SetSnapshotMode,
  WebOutputMode,
  WebPaginationUpdate,
  WebResultUpdate,
  defaultPageSize,
  webDataFromTuple
}
import edu.uci.ics.texera.web.resource.WorkflowResultService.calculateDirtyPageIndices
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource.send
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler
import edu.uci.ics.texera.workflow.operators.sink.SimpleSinkOpDesc
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode.{SET_DELTA, SET_SNAPSHOT}
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import javax.websocket.Session
import scala.collection.mutable

object WorkflowResultService {

  /**
    * Calculate which page in frontend need to be re-fetched
    * @param beforeSnapshot data before status update event (i.e. unmodified sessionResults)
    * @param afterSnapshot data after status update event
    * @return list of indices of modified pages starting from 1 (Important: index start from 1 instead of 0)
    */
  def calculateDirtyPageIndices(
      beforeSnapshot: List[ITuple],
      afterSnapshot: List[ITuple],
      pageSize: Int
  ): List[Int] = {
    var currentIndex = 1
    var currentIndexPageCount = 0
    val dirtyPageIndices = new mutable.HashSet[Int]()
    for ((before, after) <- beforeSnapshot.zipAll(afterSnapshot, null, null)) {
      if (before == null || after == null || !before.equals(after)) {
        dirtyPageIndices.add(currentIndex)
      }
      currentIndexPageCount += 1
      if (currentIndexPageCount == pageSize) {
        currentIndexPageCount = 0
        currentIndex += 1
      }
    }

    dirtyPageIndices.toList
  }
}

object OperatorResultService {

  val defaultPageSize: Int = 10

  /**
    * Behavior for different modes:
    *  - PaginationMode   (used by view result operator)
    *     - send new number of pages and dirty page index
    *  - SetSnapshotMode  (used by visualization in snapshot mode)
    *     - send entire result snapshot to frontend
    *  - SetDeltaMode     (used by visualization in snapshot mode)
    *     - send incremental delta result to frontend
    */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  sealed abstract class WebOutputMode extends Product with Serializable
  @JsonTypeName("PaginationMode")
  final case class PaginationMode() extends WebOutputMode
  @JsonTypeName("SetSnapshotMode")
  final case class SetSnapshotMode() extends WebOutputMode
  @JsonTypeName("SetDeltaMode")
  final case class SetDeltaMode() extends WebOutputMode

  sealed abstract class WebResultUpdate extends Product with Serializable
  case class WebPaginationUpdate(
      mode: PaginationMode,
      totalNumTuples: Int,
      dirtyPageIndices: List[Int]
  ) extends WebResultUpdate
  case class WebDataUpdate(mode: WebOutputMode, table: List[ObjectNode], chartType: Option[String])
      extends WebResultUpdate

  def webDataFromTuple(
      mode: WebOutputMode,
      table: List[ITuple],
      chartType: Option[String]
  ): WebDataUpdate = {
    val tableInJson = table.map(t => t.asInstanceOf[Tuple].asKeyValuePairJson())
    WebDataUpdate(mode, tableInJson, chartType)
  }
}

case class WebResultUpdateEvent(updates: Map[String, WebResultUpdate]) extends TexeraWebSocketEvent

/**
  * Service that manages the materialized result of an operator.
  * It always keeps the latest snapshot of the computation result.
  */
class OperatorResultService(val operatorID: String, val workflowCompiler: WorkflowCompiler) {

  val webOutputMode: WebOutputMode = {
    val op = workflowCompiler.workflow.getOperator(operatorID)
    if (!op.isInstanceOf[SimpleSinkOpDesc]) {
      throw new RuntimeException("operator is not sink: " + op.operatorID)
    }
    val sink = op.asInstanceOf[SimpleSinkOpDesc]
    (sink.getOutputMode, sink.getChartType) match {
      case (SET_SNAPSHOT, Some(_)) => SetSnapshotMode()
      case (SET_DELTA, Some(_))    => SetDeltaMode()
      case (_, None)               => PaginationMode()
    }
  }

  val chartType: Option[String] = {
    val op = workflowCompiler.workflow.getOperator(operatorID)
    if (!op.isInstanceOf[SimpleSinkOpDesc]) {
      throw new RuntimeException("operator is not sink: " + op.operatorID)
    }
    op.asInstanceOf[SimpleSinkOpDesc].getChartType
  }

  private var result: List[ITuple] = List()

  def convertWebResultUpdate(
      resultUpdate: OperatorResult
  ): WebResultUpdate = {
    (webOutputMode, resultUpdate.outputMode) match {
      case (PaginationMode(), SET_SNAPSHOT) =>
        val dirtyPageIndices =
          calculateDirtyPageIndices(result, resultUpdate.result, defaultPageSize)
        WebPaginationUpdate(PaginationMode(), resultUpdate.result.size, dirtyPageIndices)

      case (SetSnapshotMode(), SET_SNAPSHOT) | (SetDeltaMode(), SET_DELTA) =>
        webDataFromTuple(webOutputMode, resultUpdate.result, chartType)

      // currently not supported mode combinations
      // (PaginationMode, SET_DELTA) | (DataSnapshotMode, SET_DELTA) | (DataDeltaMode, SET_SNAPSHOT)
      case _ =>
        throw new RuntimeException(
          "update mode combination not supported: " + (webOutputMode, resultUpdate.outputMode)
        )
    }
  }

  def updateSnapshot(resultUpdate: OperatorResult): Unit = {
    resultUpdate.outputMode match {
      case SET_SNAPSHOT =>
        this.result = resultUpdate.result
      case SET_DELTA =>
        this.result = (this.result ++ resultUpdate.result)
    }
  }

  def getSnapshot: List[ITuple] = this.result

}

class WorkflowResultService(val workflowCompiler: WorkflowCompiler) {

  val operatorResults: Map[String, OperatorResultService] =
    workflowCompiler.workflow.getSinkOperators
      .map(sink => (sink, new OperatorResultService(sink, workflowCompiler)))
      .toMap

  def onResultUpdate(resultUpdate: WorkflowResultUpdate, session: Session): Unit = {

    // prepare web update event to frontend
    val webUpdateEvent = resultUpdate.operatorResults.map(e => {
      val opResultService = operatorResults(e._1)
      val webUpdateEvent = opResultService.convertWebResultUpdate(e._2)
      (e._1, webUpdateEvent)
    })

    // update the result snapshot of each operator
    resultUpdate.operatorResults.foreach(e => operatorResults(e._1).updateSnapshot(e._2))

    // send update event to frontend
    send(session, WebResultUpdateEvent(webUpdateEvent))

  }

}
