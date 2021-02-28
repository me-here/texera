package edu.uci.ics.texera.workflow.operators.hashJoinSpecial

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HashJoinSpecialOpExec[K](
    val buildTable: LinkIdentity,
    val buildAttributeName: String,
    val probeAttributeName: String,
    val saleCustomerAttr: String,
    val customerPKAttr: String
) extends OperatorExecutor {

  var isBuildTableFinished: Boolean = false
  var buildTableHashMap: mutable.HashMap[K, ArrayBuffer[Tuple]] = _
  var outputProbeSchema: Schema = _

  var currentEntry: Iterator[Tuple] = _
  var currentTuple: Tuple = _

  var monthToStatistics: mutable.HashMap[K, Int] = _

  // probe attribute removed in the output schema
  private def createOutputProbeSchema() = {
    val builder = Schema.newBuilder()
    builder.add(new Attribute("Month", AttributeType.STRING))
    builder.add(new Attribute("Sale-Count", AttributeType.INTEGER))
    builder.build()
  }

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        // The operatorInfo() in HashJoinOpDesc has a inputPorts list. In that the
        // small input port comes first. So, it is assigned the inputNum 0. Similarly
        // the large input is assigned the inputNum 1.
        if (input == buildTable) {
          val key = t.getField(buildAttributeName).asInstanceOf[K]
          var storedTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
          storedTuples += t
          buildTableHashMap.put(key, storedTuples)
          Iterator()
        } else {
          if (!isBuildTableFinished) {
            val err = WorkflowRuntimeError(
              "Probe table came before build table ended",
              "HashJoinOpExec",
              Map("stacktrace" -> Thread.currentThread().getStackTrace().mkString("\n"))
            )
            throw new WorkflowRuntimeException(err)
          } else {
            val key = t.getField(probeAttributeName).asInstanceOf[K]
            val custFromSale = t.getField(saleCustomerAttr)
            val storedTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
            if (storedTuples.isEmpty) {
              Iterator()
            } else {
              val x = storedTuples.map(buildTuple => buildTuple.getField(customerPKAttr))
              if (!x.contains(custFromSale)) {
                var counter = monthToStatistics.getOrElse(key, 0)
                counter += 1
                monthToStatistics(key) = counter
              }
            }
            Iterator()
          }
        }
      case Right(_) =>
        if (input == buildTable) {
          isBuildTableFinished = true
          if (buildTableHashMap.keySet.size < 13) {
            println(
              s"\tKeys in build table are: ${buildTableHashMap.keySet.mkString(", ")}"
            )
          }
          Iterator()
        } else {
          if (outputProbeSchema == null) {
            outputProbeSchema = createOutputProbeSchema()
          }
          var tuplesToOutput: ArrayBuffer[Tuple] = new ArrayBuffer[Tuple]()
          monthToStatistics.keys.foreach(month => {
            val builder = Tuple.newBuilder()
            builder.add(outputProbeSchema.getAttribute("Month"), month)
            builder.add(
              outputProbeSchema.getAttribute("Sale-Count"),
              monthToStatistics.getOrElse(month, 0)
            )
            tuplesToOutput.append(builder.build())
          })
          tuplesToOutput.iterator
        }
    }
  }

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, mutable.ArrayBuffer[Tuple]]()
    monthToStatistics = new mutable.HashMap[K, Int]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
    monthToStatistics.clear()
  }
}
