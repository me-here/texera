package edu.uci.ics.texera.workflow.operators.sort

import edu.uci.ics.amber.engine.common.{Constants, InputExhausted}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SortOpLocalExec(
    val sortAttributeName: String,
    val rangeMin: Float,
    val rangeMax: Float,
    val localIdx: Int,
    val numWorkers: Int
) extends OperatorExecutor {

  var sortedTuples: ArrayBuffer[Tuple] = _
  var newTuples: ArrayBuffer[Tuple] = _

  val jump: Int =
    ((rangeMax - rangeMin) / numWorkers).toInt + 1
  val workerLowerLimitIncluded: Int = jump * localIdx
  val workerUpperLimitExcluded: Int =
    if (jump * (localIdx + 1) > rangeMax) rangeMax.toInt else jump * (localIdx + 1)

  def addTupleToSortedList(tuple: Tuple, sortedList: ArrayBuffer[Tuple]): Unit = {
    if (sortedList.length == 0) {
      sortedList.append(tuple)
      return
    }

    var currIdx: Int = sortedList.length - 1
    var lastElem: Tuple = null
    while (
      currIdx >= 0 &&
      sortedList(currIdx).getField(sortAttributeName).asInstanceOf[Float] > tuple
        .getField(sortAttributeName)
        .asInstanceOf[Float]
    ) {
      if (currIdx == sortedList.length - 1) {
        lastElem = sortedList(sortedList.length - 1)
      } else {
        sortedList(currIdx + 1) = sortedList(currIdx)
      }
      currIdx -= 1
    }
    if (lastElem != null) {
      sortedList(currIdx + 1) = tuple
      sortedList.append(lastElem)
      lastElem = null
    } else {
      sortedList.append(tuple)
    }

  }

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        if (
          t.getField(sortAttributeName).asInstanceOf[Float] >= workerLowerLimitIncluded && t
            .getField(sortAttributeName)
            .asInstanceOf[Float] < workerUpperLimitExcluded
        ) {
          addTupleToSortedList(t, sortedTuples)
        } else {
          addTupleToSortedList(t, newTuples)
        }
        Iterator()
      case Right(_) =>
        sortedTuples.toIterator ++ newTuples.toIterator

    }
  }

  override def open(): Unit = {
    sortedTuples = new ArrayBuffer[Tuple]()
    newTuples = new ArrayBuffer[Tuple]()
  }

  override def close(): Unit = {
    sortedTuples.clear()
  }
}
