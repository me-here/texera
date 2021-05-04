package edu.uci.ics.texera.workflow.operators.sort

import edu.uci.ics.amber.engine.common.{Constants, InputExhausted}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

import java.util
import java.util.Comparator
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class SortOpLocalExec(
    val sortAttributeName: String,
    val rangeMin: Float,
    val rangeMax: Float,
    val localIdx: Int,
    val numWorkers: Int
) extends OperatorExecutor {

  val outputSchema: Schema =
    Schema.newBuilder().add(new Attribute(sortAttributeName, AttributeType.FLOAT)).build()

  // var sortedTuples: ArrayBuffer[Tuple] = _
  var sortedTuples: mutable.PriorityQueue[Tuple] = _

  /** For free workers receiving data of skewed workers * */
  //var tuplesFromSkewedWorker: ArrayBuffer[Tuple] = _
  var tuplesFromSkewedWorker: mutable.PriorityQueue[Float] = _
  @volatile var skewedWorkerIdentity: ActorVirtualIdentity = null

  /** For skewed worker whose data is sent to free workers * */
  @volatile var sentTuplesToFree: Boolean = false
  @volatile var receivedTuplesFromFree: Boolean = false
  var receivedFromFreeWorker: ArrayBuffer[Float] = _

  val jump: Int =
    ((rangeMax - rangeMin) / numWorkers).toInt + 1
  val workerLowerLimitIncluded: Int = jump * localIdx
  val workerUpperLimitExcluded: Int =
    if (jump * (localIdx + 1) > rangeMax) rangeMax.toInt else jump * (localIdx + 1)

  def getSortedLists(): ArrayBuffer[ArrayBuffer[Float]] = {
    val sendingLists = new ArrayBuffer[ArrayBuffer[Float]]
    var count = 1
    var curr = new ArrayBuffer[Float]

    val it = tuplesFromSkewedWorker.toIterator
    while (it.hasNext) {
      curr.append(it.next())
      if (count % Constants.eachTransferredListSize == 0) {
        sendingLists.append(curr)
        curr = new ArrayBuffer[Float]
      }
      count += 1
    }
//    for (value <- tuplesFromSkewedWorker) {
//      curr.append(value)
//      if (count % 4000 == 0) {
//        sendingLists.append(curr)
//        curr = new ArrayBuffer[Tuple]
//      }
//      count += 1
//    }
    if (!curr.isEmpty) sendingLists.append(curr)
    sendingLists
  }

  def addFloatToSortedList(f: Float, sortedList: mutable.PriorityQueue[Float]): Unit = {
    sortedList.enqueue(f)
  }

  def addTupleToSortedList(tuple: Tuple, sortedList: mutable.PriorityQueue[Tuple]): Unit = {
    val outputTuple = Tuple
      .newBuilder()
      .add(
        outputSchema.getAttribute(sortAttributeName),
        tuple.getField(sortAttributeName).asInstanceOf[Float]
      )
      .build()
    sortedList.enqueue(outputTuple)

//    if (sortedList.length == 0) {
//      sortedList.append(tuple)
//      return
//    }

//    var currIdx: Int = sortedList.length - 1
//    var lastElem: Tuple = null
//    while (
//      currIdx >= 0 &&
//      sortedList(currIdx).getField(sortAttributeName).asInstanceOf[Float] > tuple
//        .getField(sortAttributeName)
//        .asInstanceOf[Float]
//    ) {
//      if (currIdx == sortedList.length - 1) {
//        lastElem = sortedList(sortedList.length - 1)
//      } else {
//        sortedList(currIdx + 1) = sortedList(currIdx)
//      }
//      currIdx -= 1
//    }
//    if (lastElem != null) {
//      sortedList(currIdx + 1) = tuple
//      sortedList.append(lastElem)
//      lastElem = null
//    } else {
//      sortedList.append(tuple)
//    }

  }

  def outputOneList(ownList: mutable.PriorityQueue[Tuple]): Iterator[Tuple] = {
    new Iterator[Tuple] {
      override def hasNext: Boolean = ownList.size > 0

      override def next(): Tuple = ownList.dequeue()
    }
  }

  def outputMergedLists(
      ownList: mutable.PriorityQueue[Tuple],
      receivedList: ArrayBuffer[Float]
  ): Iterator[Tuple] = {
    // merge the two sorted lists
    new Iterator[Tuple] {
      var receivedIdx = 0
      override def hasNext: Boolean = {
        (ownList.size > 0 || receivedIdx < receivedList.size)
      }

      override def next(): Tuple = {
        if (ownList.size > 0 && receivedIdx < receivedList.size) {
          if (
            ownList.head
              .getField(sortAttributeName)
              .asInstanceOf[Float] < receivedList(receivedIdx)
          ) {
            return ownList.dequeue()
          } else {
            val ret = receivedList(receivedIdx)
            receivedIdx += 1
            return Tuple
              .newBuilder()
              .add(
                outputSchema.getAttribute(sortAttributeName),
                ret
              )
              .build()
          }
        } else if (ownList.size > 0) {
          return ownList.dequeue()
        } else {
          val ret = receivedList(receivedIdx)
          receivedIdx += 1
          return Tuple
            .newBuilder()
            .add(
              outputSchema.getAttribute(sortAttributeName),
              ret
            )
            .build()
        }
      }
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
          addFloatToSortedList(
            t.getField(sortAttributeName).asInstanceOf[Float],
            tuplesFromSkewedWorker
          )
        }
        Iterator()
      case Right(_) =>
        if (!sentTuplesToFree) {
          println(s"\t PRODUCED ${sortedTuples.size}")
          outputOneList(sortedTuples)
        } else {
          println(s"\t PRODUCED ${sortedTuples.size + receivedFromFreeWorker.size}")
          outputMergedLists(sortedTuples, receivedFromFreeWorker)
        }
    }
  }

  override def open(): Unit = {
    // sortedTuples = new ArrayBuffer[Tuple]()
    sortedTuples = mutable.PriorityQueue.empty[Tuple](
      Ordering
        .by[Tuple, Float](
          _.getField(sortAttributeName)
            .asInstanceOf[Float]
        )
        .reverse
    )

    tuplesFromSkewedWorker = mutable.PriorityQueue.empty[Float]

    receivedFromFreeWorker = new ArrayBuffer[Float]()
  }

  override def close(): Unit = {
    sortedTuples.clear()
    tuplesFromSkewedWorker.clear()
    receivedFromFreeWorker.clear()
  }

}
