package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.DetectSkewHandler.{
  DetectSkew,
  detectSkewLogger,
  endTimeForBuildRepl,
  endTimeForMetricColl,
  endTimeForNetChange,
  getSkewedAndFreeWorker,
  previousCallFinished,
  skewedWorkerToFreeWorker,
  startTimeForBuildRepl,
  startTimeForMetricColl,
  startTimeForNetChange
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryLoadMetricsHandler.{
  CurrentLoadMetrics,
  QueryLoadMetrics
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryNextOpLoadMetricsHandler.{
  FutureLoadMetrics,
  QueryNextOpLoadMetrics
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SendBuildTableHandler.SendBuildTable
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShareFlowHandler.ShareFlow
import edu.uci.ics.amber.engine.common.{Constants, WorkflowLogger}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DetectSkewHandler {
  var previousCallFinished = true
  var skewedWorkerToFreeWorker = new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  var startTimeForMetricColl: Long = _
  var endTimeForMetricColl: Long = _
  var startTimeForBuildRepl: Long = _
  var endTimeForBuildRepl: Long = _
  var startTimeForNetChange: Long = _
  var endTimeForNetChange: Long = _
  var detectSkewLogger: WorkflowLogger = new WorkflowLogger("DetectSkewHandler")

  final case class DetectSkew(joinLayer: WorkerLayer, probeLayer: WorkerLayer)
      extends ControlCommand[CommandCompleted]

  def getSkewedAndFreeWorker(
      loads: mutable.HashMap[ActorVirtualIdentity, Long]
  ): (ActorVirtualIdentity, ActorVirtualIdentity) = {
    val joinWorkers = loads.keys.toList
    assert(joinWorkers.size > 1)
    val skewedWorker = joinWorkers(0)
    val freeWorker = joinWorkers(1)
    if (
      skewedWorkerToFreeWorker
        .contains(skewedWorker) && skewedWorkerToFreeWorker(skewedWorker) == freeWorker
    ) {
      (null, null)
    } else {
      skewedWorkerToFreeWorker(skewedWorker) = freeWorker
      if (Constants.onlyDetectSkew) { (null, null) }
      else { (skewedWorker, freeWorker) }

    }
  }
}

// join-skew research related
trait DetectSkewHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  private def getResultsAsFuture[T](
      workerLayer: WorkerLayer,
      message: ControlCommand[T]
  ): Future[Seq[T]] = {
    val futuresArr = new ArrayBuffer[Future[T]]()
    workerLayer.workers.keys.foreach(id => {
      futuresArr.append(send(message, id))
    })
    Future.collect(futuresArr)
  }

  private def aggregateLoadMetrics(
      cmd: DetectSkew,
      metrics: (Seq[CurrentLoadMetrics], Seq[FutureLoadMetrics])
  ): mutable.HashMap[ActorVirtualIdentity, Long] = {
    val loads = new mutable.HashMap[ActorVirtualIdentity, Long]()
    for ((id, currLoad) <- cmd.joinLayer.workers.keys zip metrics._1) {
      loads(id) = currLoad.stashedBatches + currLoad.unprocessedQueueLength
      detectSkewLogger.logInfo(
        s"\tLOAD ${id} - ${currLoad.stashedBatches} stashed batches, ${currLoad.unprocessedQueueLength} internal queue"
      )
    }
    metrics._2.foreach(replyFromNetComm => {
      for ((wId, futLoad) <- replyFromNetComm.dataToSend) {
        if (loads.contains(wId)) {
          loads(wId) = loads.getOrElse(wId, 0L) + futLoad
          detectSkewLogger.logInfo(s"\tLOAD ${wId} - ${futLoad} going to arrive")
        }
      }
    })
    loads
  }

  registerHandler { (cmd: DetectSkew, sender) =>
    {
      if (previousCallFinished) {
        previousCallFinished = false
        startTimeForMetricColl = System.nanoTime()
        Future
          .join(
            getResultsAsFuture(cmd.joinLayer, QueryLoadMetrics()),
            getResultsAsFuture(cmd.probeLayer, QueryNextOpLoadMetrics())
          )
          .flatMap(metrics => {
            endTimeForMetricColl = System.nanoTime()
            detectSkewLogger.logInfo(
              s"\tThe metrics have been collected in ${(endTimeForMetricColl - startTimeForMetricColl) / 1e9d}s"
            )
            val loads = aggregateLoadMetrics(cmd, metrics)
            detectSkewLogger.logInfo(s"\tThe final loads map ${loads.mkString("\n\t\t")}")

            val skewedAndFreeWorkers = getSkewedAndFreeWorker(loads)
            if (skewedAndFreeWorkers._1 != null && skewedAndFreeWorkers._2 != null) {
              startTimeForBuildRepl = System.nanoTime()
              send(SendBuildTable(skewedAndFreeWorkers._2), skewedAndFreeWorkers._1).flatMap(
                res => {
                  endTimeForBuildRepl = System.nanoTime()
                  detectSkewLogger.logInfo(
                    s"\tBUILD TABLE COPIED in ${(endTimeForBuildRepl - startTimeForBuildRepl) / 1e9d}s from ${skewedAndFreeWorkers._1} to ${skewedAndFreeWorkers._2}"
                  )

                  startTimeForNetChange = System.nanoTime()
                  getResultsAsFuture(
                    cmd.probeLayer,
                    ShareFlow(skewedAndFreeWorkers._1, skewedAndFreeWorkers._2)
                  ).map(seq => {
                    endTimeForNetChange = System.nanoTime()
                    detectSkewLogger.logInfo(
                      s"\tTHE NETWORK SHARE HAS HAPPENED in ${(endTimeForNetChange - startTimeForNetChange) / 1e9d}s from ${skewedAndFreeWorkers._1} to ${skewedAndFreeWorkers._2}"
                    )
                    previousCallFinished = true
                    CommandCompleted()
                  })
                }
              )
            } else {
              previousCallFinished = true
              Future { CommandCompleted() }
            }
          })
      } else { Future { CommandCompleted() } }
    }
  }

}
