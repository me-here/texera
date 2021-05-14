package edu.uci.ics.amber.engine.recovery

import java.util
import java.util.concurrent.{ExecutorService, Executors, Future, LinkedBlockingQueue}

import akka.actor.ActorRef
import com.google.common.collect.Queues
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkAckManager
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{NetworkSenderActorRef, UpdateCursorForOutput}
import edu.uci.ics.amber.engine.common.ambermessage.{ControlLogPayload, DPCursor, FromSender, LogWriterPayload, UpdateStepCursor, ShutdownWriter, WorkflowControlMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

import scala.collection.mutable

class ParallelLogWriter(
    storage: LogStorage,
    networkCommunicationActor: NetworkSenderActorRef,
    networkControlAckManager: NetworkAckManager,
    networkDataAckManager: NetworkAckManager = null
) {

  private var persistedStepCursor = storage.getStepCursor
  private var cursorUpdated = false
  private var logTime = 0L

  private var loggedControlCounter = 0
  private var loggedDataCounter = 0
  private val loggedControlSeqDelta = mutable.HashMap[VirtualIdentity, Long]().withDefaultValue(0L)
  private val loggedDataSeqDelta = mutable.HashMap[VirtualIdentity, Long]().withDefaultValue(0L)

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
          var start = 0L
          if (buffer.isEmpty) {
            // instead of using Thread.sleep(200),
            // we wait until 1 record has been pushed into the queue
            // then write this record and commit
            // during this process, we accumulate log records in the queue
            val logRecord = logRecordQueue.take()
            start = System.currentTimeMillis()
            cursorUpdated = false //invalidate flag
            writeLogRecord(logRecord)
            persistStepCursor()
            storage.commit()
          } else {
            if (buffer.get(buffer.size() - 1) == ShutdownWriter) {
              buffer.remove(buffer.size() - 1)
              isEnded = true
            }
            start = System.currentTimeMillis()
            cursorUpdated = false // invalidate flag
            batchWrite(buffer)
            persistStepCursor()
            //println(s"writing ${buffer.size} logs at a time")
            buffer.clear()
          }
          logTime += System.currentTimeMillis() - start
          // notify network actor for counter update
          networkCommunicationActor ! UpdateCursorForOutput(persistedStepCursor)
          releaseAcks(networkControlAckManager, loggedControlCounter, loggedControlSeqDelta)
          loggedControlCounter = 0
          if (networkDataAckManager != null) {
            releaseAcks(networkDataAckManager, loggedDataCounter, loggedDataSeqDelta)
            loggedDataCounter = 0
          }
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

  private def releaseAcks(
      ackManager: NetworkAckManager,
      loggedCounter: Int,
      loggedSeqDelta: mutable.Map[VirtualIdentity, Long]
  ): Unit = {
    try {
      ackManager.releaseAcks(loggedCounter)
      loggedSeqDelta.foreach(pair => ackManager.advanceSeq(pair._1, pair._2))
      loggedSeqDelta.clear()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  private def writeLogRecord(record: LogWriterPayload): Unit = {
    record match {
      case clr: ControlLogPayload =>
        storage.write(clr)
        loggedControlCounter += 1
        loggedControlSeqDelta(clr.virtualId) += 1
      case cursor: DPCursor =>
        persistedStepCursor = cursor.idx
        storage.write(cursor)
      case record @ FromSender(id) =>
        storage.write(record)
        if (networkDataAckManager != null) {
          loggedDataCounter += 1
          loggedDataSeqDelta(id) += 1
        }
      case UpdateStepCursor(cursor) =>
        //only write the last step cursor of batched log entries
        if(cursor > persistedStepCursor){
          cursorUpdated = true
          persistedStepCursor = cursor
        }
      case ShutdownWriter =>
      //skip
    }
  }

  private def persistStepCursor(): Unit ={
    if(cursorUpdated){
      storage.write(UpdateStepCursor(persistedStepCursor))
    }
  }

}
