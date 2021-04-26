package edu.uci.ics.amber.engine.recovery

import java.util
import java.util.concurrent.{ExecutorService, Executors, Future, LinkedBlockingQueue}

import akka.actor.ActorRef
import com.google.common.collect.Queues
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkSenderActorRef,
  UpdateCountForOutput
}
import edu.uci.ics.amber.engine.common.ambermessage.{
  DPCursor,
  DataBatchSequence,
  LogWriterPayload,
  ShutdownWriter,
  UpdateCountForInput,
  WorkflowControlMessage
}

class ParallelLogWriter(
    storage: LogStorage,
    mainActor: ActorRef,
    networkCommunicationActor: NetworkSenderActorRef,
    isController: Boolean = false
) {

  private var dataReceivedCounter = 0L
  private var dataProcessedCounter = 0L
  private var controlProcessedCounter = 0L
  private var controlReceivedCounter = 0L
  private var logTime = 0L

  def addLogRecord(logRecord: LogWriterPayload): Unit = {
    if (!storage.isInstanceOf[EmptyLogStorage]) {
      logRecordQueue.put(logRecord)
    }
  }

  val logRecordQueue: LinkedBlockingQueue[LogWriterPayload] = Queues.newLinkedBlockingQueue()

  private val loggingExecutor: ExecutorService = Executors.newSingleThreadExecutor
  if (!storage.isInstanceOf[EmptyLogStorage]) {
    loggingExecutor.submit(new Runnable() {
      def run(): Unit = {
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY)
        var isEnded = false
        val buffer = new util.ArrayList[LogWriterPayload]()
        while (!isEnded) {
          logRecordQueue.drainTo(buffer)
          val start = System.currentTimeMillis()
          if (buffer.isEmpty) {
            // instead of using Thread.sleep(200),
            // we wait until 1 record has been pushed into the queue
            // then write this record and commit
            // during this process, we accumulate log records in the queue
            val logRecord = logRecordQueue.take()
            writeLogRecord(logRecord)
            storage.commit()
          } else {
            if (buffer.get(buffer.size() - 1) == ShutdownWriter) {
              buffer.remove(buffer.size() - 1)
              isEnded = true
            }
            batchWrite(buffer)
            //println(s"writing ${buffer.size} logs at a time")
            buffer.clear()
          }
          logTime += System.currentTimeMillis() - start
          // notify network actor for counter update
          networkCommunicationActor ! UpdateCountForOutput(
            dataProcessedCounter,
            controlProcessedCounter
          )
          // notify main actor for counter update
          mainActor ! UpdateCountForInput(dataReceivedCounter, controlReceivedCounter)
        }
        storage.release()
      }
    })
  }

  def shutdown(): Unit = {
    if (!storage.isInstanceOf[EmptyLogStorage]) {
      logRecordQueue.put(ShutdownWriter)
    }
    println(s"log time for ${storage.id} = ${logTime / 1000f}")
    loggingExecutor.shutdownNow()
  }

  private def batchWrite(buffer: util.ArrayList[LogWriterPayload]): Unit = {
    buffer.stream().forEach(writeLogRecord)
    storage.commit()
  }

  private def writeLogRecord(record: LogWriterPayload): Unit = {
    record match {
      case clr: WorkflowControlMessage =>
        storage.writeControlLogRecord(clr)
        controlReceivedCounter += 1
        if (isController) controlProcessedCounter += 1
      case DPCursor(idx) =>
        storage.writeDPLogRecord(idx)
        controlProcessedCounter += 1
      case DataBatchSequence(id, batchSize, seq) =>
        storage.writeDataLogRecord(id, seq)
        dataProcessedCounter += batchSize
        dataReceivedCounter += 1
      case ShutdownWriter =>
      //skip
    }
  }

}
