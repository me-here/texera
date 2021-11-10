package edu.uci.ics.texera.workflow.operators.hashJoinTweets

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HashJoinTweetsOpExec[K](
    override val buildTable: LinkIdentity,
    override val buildAttributeName: String,
    override val probeAttributeName: String,
    val tweetTextAttr: String,
    val slangTextAttr: String
) extends HashJoinOpExec[K](buildTable, buildAttributeName, probeAttributeName) {

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
            val storedTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
            if (storedTuples.isEmpty) {
              Iterator()
            } else {
              var count1 = 0;
              for (i <- 0 to 50) {
                val tweetText1 = t.getField(tweetTextAttr).asInstanceOf[String]
                val x1 = storedTuples(0).getField(slangTextAttr).asInstanceOf[String]
                if (tweetText1.contains(x1 + i.toString())) {
                  count1 += 1
                }
              }
              if (count1 > 100) {
                return Iterator()
              }
              val tweetText = t.getField(tweetTextAttr).asInstanceOf[String]
              val x =
                storedTuples(0).getField(slangTextAttr).asInstanceOf[String]
              if (tweetText.toLowerCase().contains(x.toLowerCase())) {
                Iterator(t)
              } else {
                Iterator()
              }
            }
          }
        }
      case Right(_) =>
        if (input == buildTable) {
          isBuildTableFinished = true
//          if (buildTableHashMap.keySet.size < 13) {
//            println(
//              s"\tKeys in build table are: ${buildTableHashMap.keySet.mkString(", ")}"
//            )
//          }
        }
        Iterator()
    }
  }

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, mutable.ArrayBuffer[Tuple]]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
  }
}
