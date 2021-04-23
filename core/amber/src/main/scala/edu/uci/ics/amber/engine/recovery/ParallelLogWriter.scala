package edu.uci.ics.amber.engine.recovery

import java.util
import java.util.concurrent.{ExecutorService, Executors, Future, LinkedBlockingQueue}

import com.google.common.collect.Queues
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{DisableCountCheck, NetworkSenderActorRef, UpdateCount}
import edu.uci.ics.amber.engine.common.ambermessage.{DPCursor, DataBatchSequence, LogWriterPayload, ShutdownWriter, WorkflowControlMessage}

class ParallelLogWriter(storage: LogStorage, networkCommunicationActor: NetworkSenderActorRef, isController:Boolean = false) {

  private var dataCounter = 0L
  private var controlCounter = 0L
  private var logTime = 0L

  def addLogRecord(logRecord:LogWriterPayload):Unit = {
    if(!storage.isInstanceOf[EmptyLogStorage]){
      logRecordQueue.put(logRecord)
    }
  }

  val logRecordQueue: LinkedBlockingQueue[LogWriterPayload] = Queues.newLinkedBlockingQueue()

  private val loggingExecutor: ExecutorService = Executors.newSingleThreadExecutor
  if(!storage.isInstanceOf[EmptyLogStorage]) {
    loggingExecutor.submit(new Runnable() {
      def run(): Unit = {
        var isEnded = false
        val buffer = new util.ArrayList[LogWriterPayload]()
        while (!isEnded) {
          logRecordQueue.drainTo(buffer)
          if (buffer.isEmpty) {
            Thread.sleep(200)
          } else {
            val start = System.currentTimeMillis()
            if (buffer.get(buffer.size() - 1) == ShutdownWriter) {
              buffer.remove(buffer.size() - 1)
              isEnded = true
            }
            batchWrite(buffer)
            //println(s"writing ${buffer.size} logs at a time")
            buffer.clear()
            // notify network actor for counter update
            networkCommunicationActor ! UpdateCount(dataCounter, controlCounter)
            logTime += System.currentTimeMillis() - start
          }
        }
        storage.release()
      }
    })
  }else{
    networkCommunicationActor ! DisableCountCheck
  }

  def shutdown(): Unit ={
    if(!storage.isInstanceOf[EmptyLogStorage]) {
      logRecordQueue.put(ShutdownWriter)
    }
    println(s"log time for ${storage.id} = ${logTime/1000f}")
    loggingExecutor.shutdownNow()
  }


  private def batchWrite(buffer:util.ArrayList[LogWriterPayload]): Unit ={
    buffer.stream().forEach{
      case clr:WorkflowControlMessage =>
        storage.writeControlLogRecord(clr)
        if(isController)controlCounter += 1
      case DPCursor(idx) =>
        storage.writeDPLogRecord(idx)
        controlCounter += 1
      case dlr @ DataBatchSequence(id,batchSize) =>
        storage.writeDataLogRecord(id)
        dataCounter += batchSize
      case ShutdownWriter =>
        //skip
    }
    storage.commit()
  }


}
