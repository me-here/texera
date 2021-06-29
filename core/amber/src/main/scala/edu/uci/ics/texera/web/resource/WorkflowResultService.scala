package edu.uci.ics.texera.web.resource

import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.amber.engine.architecture.principal.OperatorResult
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.web.model.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.resource.OperatorResultService.{
  PaginationMode,
  SetDeltaMode,
  SetSnapshotMode,
  WebResultUpdateMode,
  defaultPageSize
}
import edu.uci.ics.texera.web.resource.WorkflowResultService.calculateDirtyPageIndices
//import edu.uci.ics.texera.web.resource.OperatorResultService.{PaginationMode, WebResultUpdateMode}
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode.{SET_DELTA, SET_SNAPSHOT}

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

  sealed abstract class WebResultUpdateMode extends Product with Serializable
  final case class PaginationMode() extends WebResultUpdateMode
  final case class SetSnapshotMode() extends WebResultUpdateMode
  final case class SetDeltaMode() extends WebResultUpdateMode

  case class WebPaginationUpdate(mode: PaginationMode, numPages: Int, dirtyPageIndices: List[Int])
  case class WebDataUpdate(
      mode: WebResultUpdateMode,
      table: List[ObjectNode],
      chartType: Option[String]
  )

  case class WebResultUpdateEvent(updates: Map[String, Either[WebPaginationUpdate, WebDataUpdate]])
      extends TexeraWebSocketEvent
}

/**
  * Service that manages the materialized result of an operator.
  *
  * This service always keep the latest snapshot of the computation result.
  *
  * Behavior for different modes:
  *  - PaginationMode (used by view result operator)
  *     - engine sends update in SET_SNAPSHOT mode
  *     - send new number of pages and dirty page index
  *  - Visualization Operator in SET_SNAPSHOT mode:
  *     - send entire result snapshot to frontend
  *  - Visualization Operator in SET_DELTA mode:
  *     - send incremental delta result to frontend
  */
class OperatorResultService(val operatorID: String, val updateMode: WebResultUpdateMode) {

  private var result: List[ITuple] = List()

  def onResultUpdate(resultUpdate: OperatorResult): Unit = {
    (updateMode, resultUpdate.outputMode) match {
      case (PaginationMode(), SET_SNAPSHOT) => {
        val dirtyPageIndices =
          calculateDirtyPageIndices(result, resultUpdate.result, defaultPageSize)
        val newNumPages = math.ceil(resultUpdate.result.size.toDouble / defaultPageSize).toInt

        result = resultUpdate.result
      }

      case (SetSnapshotMode(), SET_SNAPSHOT) =>
        result = resultUpdate.result

      case (SetDeltaMode(), SET_DELTA) =>

      // currently not supported mode combinations
      // (PaginationMode, SET_DELTA) | (DataSnapshotMode, SET_DELTA) | (DataDeltaMode, SET_SNAPSHOT)
      case _ =>
        throw new RuntimeException(
          "update mode combination not supported: " + (updateMode, resultUpdate.outputMode)
        )
    }
  }

  def getSnapshot: List[ITuple] = this.result

}

class WorkflowResultService {}
