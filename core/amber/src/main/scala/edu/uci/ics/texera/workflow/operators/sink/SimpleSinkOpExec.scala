package edu.uci.ics.texera.workflow.operators.sink

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.{ITupleSinkOperatorExecutor, InputExhausted}
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable

class SimpleSinkOpExec extends ITupleSinkOperatorExecutor {

  val results: mutable.ListBuffer[ITuple] = mutable.ListBuffer()

  def getResultTuples(): Array[ITuple] = {
    results.toArray
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(
      tuple: Either[ITuple, InputExhausted],
      input: Int
  ): scala.Iterator[ITuple] = {
    tuple match {
      case Left(t) =>
        t match {
          case tuple1: Tuple if tuple1.getSchema.containsAttribute("__multiplicity__") =>
            updateResult(tuple1)
          case _ =>
            this.results += t
        }
        Iterator()
      case Right(_) =>
        results.foreach(r => println(r))
        Iterator()
    }
  }

  private def updateResult(tuple: Tuple): Unit = {
    var multiplicity = tuple.getField[Integer]("__multiplicity__")
    val tupleOriginal = Tuple.newBuilder().add(tuple).remove("__multiplicity__").build()
    while (multiplicity != 0) {
      if (multiplicity > 0) {
        results += tupleOriginal
        multiplicity -= 1
      } else {
        results -= tupleOriginal
        multiplicity += 1
      }
    }
  }

}
