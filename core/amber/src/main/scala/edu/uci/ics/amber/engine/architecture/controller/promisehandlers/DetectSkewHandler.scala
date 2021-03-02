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
  isfreeGettingSkewed,
  previousCallFinished,
  skewedWorkerToFreeWorkerCurr,
  startTimeForBuildRepl,
  startTimeForMetricColl,
  startTimeForNetChange,
  startTimeForNetRollback
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
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.RollbackFlowHandler.RollbackFlow
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SendBuildTableHandler.SendBuildTable
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShareFlowHandler.ShareFlow
import edu.uci.ics.amber.engine.common.{Constants, WorkflowLogger}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

object DetectSkewHandler {
  var previousCallFinished = true
  var startTimeForMetricColl: Long = _
  var endTimeForMetricColl: Long = _
  var startTimeForBuildRepl: Long = _
  var endTimeForBuildRepl: Long = _
  var startTimeForNetChange: Long = _
  var endTimeForNetChange: Long = _
  var startTimeForNetRollback: Long = _
  var endTimeForNetRollback: Long = _
  var detectSkewLogger: WorkflowLogger = new WorkflowLogger("DetectSkewHandler")

  var skewedWorkerToFreeWorkerCurr =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  var skewedWorkerToFreeWorkerHistory =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  var workerToLoadHistory = new mutable.HashMap[ActorVirtualIdentity, ListBuffer[Long]]()
  val historyLimit = 1

  final case class DetectSkew(joinLayer: WorkerLayer, probeLayer: WorkerLayer)
      extends ControlCommand[CommandCompleted]

  def isfreeGettingSkewed(
      loads: mutable.HashMap[ActorVirtualIdentity, Long]
  ): (ActorVirtualIdentity, ActorVirtualIdentity) = {
    val sortedWorkers = loads.keys.toList.sortBy(loads(_))
    val freeGettingSkewedWorker: ActorVirtualIdentity = sortedWorkers(sortedWorkers.size - 1)
    if (
      skewedWorkerToFreeWorkerCurr.size != 0 && skewedWorkerToFreeWorkerCurr.values.toList.contains(
        freeGettingSkewedWorker
      )
    ) {
      var actualSkewedWorker: ActorVirtualIdentity = null
      skewedWorkerToFreeWorkerCurr.keys.foreach(sw => {
        if (skewedWorkerToFreeWorkerCurr(sw) == freeGettingSkewedWorker) { actualSkewedWorker = sw }
      })
      assert(actualSkewedWorker != null)
      var isSkewed = true
      val historyOfActualSkewed = workerToLoadHistory(actualSkewedWorker)
      val historyOfFreeGettingSkewed = workerToLoadHistory(freeGettingSkewedWorker)
      assert(historyOfActualSkewed.size == historyOfFreeGettingSkewed.size)
      for (j <- 0 to historyOfFreeGettingSkewed.size - 1) {
        if (historyOfFreeGettingSkewed(j) < historyOfActualSkewed(j)) {
          isSkewed = false
        }
      }
      if (isSkewed && !Constants.onlyDetectSkew) {
        skewedWorkerToFreeWorkerCurr.remove(actualSkewedWorker)
        return (actualSkewedWorker, freeGettingSkewedWorker)
      } else { return (null, null) }
    } else { return (null, null) }
  }

  def getSkewedAndFreeWorker(
      loads: mutable.HashMap[ActorVirtualIdentity, Long]
  ): (ActorVirtualIdentity, ActorVirtualIdentity) = {
    loads.keys.foreach(worker => {
      val history = workerToLoadHistory.getOrElse(worker, new ListBuffer[Long]())
      if (history.size == historyLimit) {
        history.remove(0)
      }
      history.append(loads(worker))
      workerToLoadHistory(worker) = history
    })

    // Get workers in increasing load
    val sortedWorkers = loads.keys.toList.sortBy(loads(_))
    val currSkewedWorkers = skewedWorkerToFreeWorkerCurr.keySet
    val currFreeWorkers = skewedWorkerToFreeWorkerCurr.values.toList
    val freeWorkersThatSharedLoadInPast = skewedWorkerToFreeWorkerHistory.values.toList

    var skewedWorker: ActorVirtualIdentity = null
    var freeWorker: ActorVirtualIdentity = null

    breakable {
      for (i <- sortedWorkers.size - 1 to 0 by -1) {
        if (
          skewedWorkerToFreeWorkerCurr.size == 0 || (!currSkewedWorkers
            .contains(sortedWorkers(i)) && !currFreeWorkers.contains(
            sortedWorkers(i)
          ) && !freeWorkersThatSharedLoadInPast.contains(sortedWorkers(i)))
        ) {
          skewedWorker = sortedWorkers(i)
          break
        }
      }
    }

    // if load from skewed worker has previously been given to some worker then choose that worker, otherwise
    //  choose the worker with lowest load that hasn't been assigned to anyone.
    if (
      skewedWorkerToFreeWorkerHistory.size > 0 && skewedWorker != null && skewedWorkerToFreeWorkerHistory.keySet
        .contains(
          skewedWorker
        )
    ) {
      freeWorker = skewedWorkerToFreeWorkerHistory(skewedWorker)
    } else {
      breakable {
        for (i <- 0 to sortedWorkers.size - 1) {
          if (
            skewedWorkerToFreeWorkerCurr.size == 0 || (!currSkewedWorkers
              .contains(sortedWorkers(i)) && !currFreeWorkers.contains(sortedWorkers(i)))
          ) {
            freeWorker = sortedWorkers(i)
            break
          }
        }
      }
    }

    if (freeWorker != null && skewedWorker != null) {
      var isSkewed = true
      val historyOfSkewCand = workerToLoadHistory(skewedWorker)
      val historyOfFreeCand = workerToLoadHistory(freeWorker)
      assert(historyOfSkewCand.size == historyOfFreeCand.size)
      for (j <- 0 to historyOfSkewCand.size - 1) {
        if (historyOfSkewCand(j) < 100 || historyOfFreeCand(j) * 2 > historyOfSkewCand(j)) {
          isSkewed = false
        }
      }
      if (isSkewed && !Constants.onlyDetectSkew) {
        skewedWorkerToFreeWorkerCurr(skewedWorker) = freeWorker
        skewedWorkerToFreeWorkerHistory(skewedWorker) = freeWorker
        return (skewedWorker, freeWorker)
      } else { return (null, null) }
    } else {
      return (null, null)
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
              detectSkewLogger.logInfo(
                s"\tSkewed Worker:${skewedAndFreeWorkers._1}, Free Worker:${skewedAndFreeWorkers._2}"
              )
              startTimeForBuildRepl = System.nanoTime()
              send(SendBuildTable(skewedAndFreeWorkers._2), skewedAndFreeWorkers._1).flatMap(
                res => {
                  endTimeForBuildRepl = System.nanoTime()
                  detectSkewLogger.logInfo(
                    s"\tBUILD TABLE COPIED in ${(endTimeForBuildRepl - startTimeForBuildRepl) / 1e9d}s"
                  )

                  startTimeForNetChange = System.nanoTime()
                  getResultsAsFuture(
                    cmd.probeLayer,
                    ShareFlow(skewedAndFreeWorkers._1, skewedAndFreeWorkers._2)
                  ).map(seq => {
                    endTimeForNetChange = System.nanoTime()
                    detectSkewLogger.logInfo(
                      s"\tTHE NETWORK SHARE HAS HAPPENED in ${(endTimeForNetChange - startTimeForNetChange) / 1e9d}s"
                    )
                    previousCallFinished = true
                    CommandCompleted()
                  })
                }
              )
            } else {
              val actualSkewedAndFreeGettingSkewedWorkers = isfreeGettingSkewed(loads)
              if (
                actualSkewedAndFreeGettingSkewedWorkers._1 != null && actualSkewedAndFreeGettingSkewedWorkers._2 != null
              ) {
                detectSkewLogger.logInfo(
                  s"\tFree Worker Getting skewed:${actualSkewedAndFreeGettingSkewedWorkers._2}, Actual skewed Worker:${actualSkewedAndFreeGettingSkewedWorkers._1}"
                )
                startTimeForNetRollback = System.nanoTime()
                getResultsAsFuture(
                  cmd.probeLayer,
                  RollbackFlow(
                    actualSkewedAndFreeGettingSkewedWorkers._1,
                    actualSkewedAndFreeGettingSkewedWorkers._2
                  )
                ).map(seq => {
                  startTimeForNetRollback = System.nanoTime()
                  detectSkewLogger.logInfo(
                    s"\tTHE NETWORK ROLLBACK HAS HAPPENED in ${(endTimeForNetChange - startTimeForNetChange) / 1e9d}s"
                  )
                  previousCallFinished = true
                  CommandCompleted()
                })
              } else {
                previousCallFinished = true
                Future { CommandCompleted() }
              }
            }
          })
      } else { Future { CommandCompleted() } }
    }
  }

}
