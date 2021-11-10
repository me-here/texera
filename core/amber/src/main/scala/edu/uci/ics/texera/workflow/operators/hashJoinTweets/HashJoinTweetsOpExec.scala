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

  var slangsHashMap: mutable.HashMap[K, ArrayBuffer[String]] = _

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
          var storedWords = slangsHashMap.getOrElse(key, new ArrayBuffer[String]())
          val individualSlangWords = t.getField(slangTextAttr).asInstanceOf[String].split(':')
          individualSlangWords.foreach(w => storedWords.append(w))
          slangsHashMap.put(key, storedWords)
          println(s"Build hash table size for stateID ${key.asInstanceOf[String]} = ${storedWords.size}")
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
            val storedWords = slangsHashMap.getOrElse(key, new ArrayBuffer[String]())
            if (storedWords.isEmpty) {
              Iterator()
            } else {
              val tweetText = t.getField(tweetTextAttr).asInstanceOf[String]
              var countPresent: Int = 0
              for(i <- 0 to 10) {
                storedWords.foreach(slang => {
                  if (tweetText.toLowerCase().contains(slang.toLowerCase())) {
                    countPresent += 1
                  }
                })
              }

              if (countPresent > 3) {
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
    slangsHashMap = new mutable.HashMap[K, mutable.ArrayBuffer[String]]()
  }

  override def close(): Unit = {
    slangsHashMap.clear()
  }
}
