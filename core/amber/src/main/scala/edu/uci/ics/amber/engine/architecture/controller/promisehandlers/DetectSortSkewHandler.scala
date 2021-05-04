package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.DetectSortSkewHandler.{
  DetectSortSkew,
  detectSortSkewLogger,
  endTimeForMetricColl,
  endTimeForNetChange,
  endTimeForNotification,
  getSkewedAndFreeWorkers,
  isfreeGettingSkewed,
  previousCallFinished,
  startTimeForMetricColl,
  startTimeForNetChange,
  startTimeForNetRollback,
  startTimeForNotification
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
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SendStateTransferNotificationHandler.SendStateTranferNotification
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShareFlowHandler.ShareFlow
import edu.uci.ics.amber.engine.common.{Constants, WorkflowLogger}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

object DetectSortSkewHandler {
  var previousCallFinished = true
  var startTimeForMetricColl: Long = _
  var endTimeForMetricColl: Long = _
  var startTimeForNotification: Long = _
  var endTimeForNotification: Long = _
  var startTimeForNetChange: Long = _
  var endTimeForNetChange: Long = _
  var startTimeForNetRollback: Long = _
  var endTimeForNetRollback: Long = _
  var detectSortSkewLogger: WorkflowLogger = new WorkflowLogger("DetectSortSkewHandler")

  var skewedWorkerToFreeWorkerCurr =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  var skewedWorkerToFreeWorkerHistory =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  var workerToLoadHistory = new mutable.HashMap[ActorVirtualIdentity, ListBuffer[Long]]()
  val historyLimit = 1

  final case class DetectSortSkew(sortLayer: WorkerLayer, prevLayer: WorkerLayer)
      extends ControlCommand[CommandCompleted]

  def updateLoadHistory(loads: mutable.HashMap[ActorVirtualIdentity, Long]): Unit = {
    loads.keys.foreach(worker => {
      val history = workerToLoadHistory.getOrElse(worker, new ListBuffer[Long]())
      if (history.size == historyLimit) {
        history.remove(0)
      }
      history.append(loads(worker))
      workerToLoadHistory(worker) = history
    })
  }

  def isEligibleForSkewed(worker: ActorVirtualIdentity): Boolean = {
    (skewedWorkerToFreeWorkerCurr.size == 0 || (!skewedWorkerToFreeWorkerCurr.keySet
      .contains(worker) && !skewedWorkerToFreeWorkerCurr.values.toList
      .contains(
        worker
      ))) && (skewedWorkerToFreeWorkerHistory.size == 0 || !skewedWorkerToFreeWorkerHistory.values.toList
      .contains(worker))
  }

  def isEligibleForFree(worker: ActorVirtualIdentity): Boolean = {
    (skewedWorkerToFreeWorkerCurr.size == 0 || (!skewedWorkerToFreeWorkerCurr.keySet
      .contains(worker) && !skewedWorkerToFreeWorkerCurr.values.toList
      .contains(
        worker
      ))) && (skewedWorkerToFreeWorkerHistory.size == 0 || !skewedWorkerToFreeWorkerHistory.values.toList
      .contains(worker))
  }

  def passSkewTest(
      skewedWorkerCand: ActorVirtualIdentity,
      freeWorkerCand: ActorVirtualIdentity,
      multiplier: Double
  ): Boolean = {
    var isSkewed = true
    val skewedHist = workerToLoadHistory(skewedWorkerCand)
    val freeHist = workerToLoadHistory(freeWorkerCand)
    assert(skewedHist.size == freeHist.size)
    for (j <- 0 to skewedHist.size - 1) {
      if (skewedHist(j) < 100 || skewedHist(j) < multiplier * freeHist(j)) {
        isSkewed = false
      }
    }
    isSkewed
  }

  // return is array of actual skewed worker and free getting skewed
  def isfreeGettingSkewed(
      loads: mutable.HashMap[ActorVirtualIdentity, Long]
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)] = {
    val ret = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)]()
    val sortedWorkers = loads.keys.toList.sortBy(loads(_))
    val freeWorkersBeingUsed = skewedWorkerToFreeWorkerCurr.values.toList
    for (i <- 0 to sortedWorkers.size - 1) {
      if (
        skewedWorkerToFreeWorkerCurr.size != 0 && freeWorkersBeingUsed.contains(sortedWorkers(i))
      ) {
        var actualSkewedWorker: ActorVirtualIdentity = null
        skewedWorkerToFreeWorkerCurr.keys.foreach(sw => {
          if (skewedWorkerToFreeWorkerCurr(sw) == sortedWorkers(i)) { actualSkewedWorker = sw }
        })
        assert(actualSkewedWorker != null)

        if (!Constants.onlyDetectSkew && passSkewTest(sortedWorkers(i), actualSkewedWorker, 1.5)) {
          ret.append((actualSkewedWorker, sortedWorkers(i)))
          skewedWorkerToFreeWorkerCurr.remove(actualSkewedWorker)
        }
      }
    }
    ret
  }

  // return value is array of (skewedWorker, freeWorker, whether state replication has to be done)
  def getSkewedAndFreeWorkers(
      loads: mutable.HashMap[ActorVirtualIdentity, Long]
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)] = {
    updateLoadHistory(loads)
    val ret = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)]()
    // Get workers in increasing load
    val sortedWorkers = loads.keys.toList.sortBy(loads(_))
    val currSkewedWorkers = skewedWorkerToFreeWorkerCurr.keySet
    val currFreeWorkers = skewedWorkerToFreeWorkerCurr.values.toList
    val freeWorkersThatSharedLoadInPast = skewedWorkerToFreeWorkerHistory.values.toList

    for (i <- sortedWorkers.size - 1 to 0 by -1) {
      if (isEligibleForSkewed(sortedWorkers(i))) {
        if (
          skewedWorkerToFreeWorkerHistory.size > 0 && skewedWorkerToFreeWorkerHistory.keySet
            .contains(sortedWorkers(i))
        ) {
          if (
            passSkewTest(sortedWorkers(i), skewedWorkerToFreeWorkerHistory(sortedWorkers(i)), 2)
          ) {
            ret.append(
              (sortedWorkers(i), skewedWorkerToFreeWorkerHistory(sortedWorkers(i)), false)
            )
            skewedWorkerToFreeWorkerCurr(sortedWorkers(i)) = skewedWorkerToFreeWorkerHistory(
              sortedWorkers(i)
            )
          }
        } else if (i > 0) {
          breakable {
            for (j <- 0 to i - 1) {
              if (
                isEligibleForFree(sortedWorkers(j)) && passSkewTest(
                  sortedWorkers(i),
                  sortedWorkers(j),
                  2
                )
              ) {
                ret.append((sortedWorkers(i), sortedWorkers(j), true))
                skewedWorkerToFreeWorkerCurr(sortedWorkers(i)) = sortedWorkers(j)
                skewedWorkerToFreeWorkerHistory(sortedWorkers(i)) = sortedWorkers(j)
                break
              }
            }
          }
        }
      }
    }

    if (Constants.onlyDetectSkew) {
      return new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)]()
    } else {
      return ret
    }
  }

}

// join-skew research related
trait DetectSortSkewHandler {
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

  private def getShareFlowResultsAsFuture[T](
      workerLayer: WorkerLayer,
      skewedAndFreeWorkersList: ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)]
  ): Future[Seq[Map[ActorVirtualIdentity, Long]]] = {
    val futuresArr = new ArrayBuffer[Future[Map[ActorVirtualIdentity, Long]]]()
    skewedAndFreeWorkersList.foreach(sf => {
      workerLayer.workers.keys.foreach(id => {
        futuresArr.append(send(ShareFlow(sf._1, sf._2), id))
      })
    })
    Future.collect(futuresArr)
  }

  private def getRollbackFlowResultsAsFuture[T](
      workerLayer: WorkerLayer,
      actualSkewedAndFreeWorkersList: ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)]
  ): Future[Seq[Map[ActorVirtualIdentity, Long]]] = {
    val futuresArr = new ArrayBuffer[Future[Map[ActorVirtualIdentity, Long]]]()
    actualSkewedAndFreeWorkersList.foreach(sf => {
      workerLayer.workers.keys.foreach(id => {
        futuresArr.append(send(RollbackFlow(sf._1, sf._2), id))
      })
    })
    Future.collect(futuresArr)
  }

  private def aggregateLoadMetrics(
      cmd: DetectSortSkew,
      metrics: (Seq[CurrentLoadMetrics], Seq[FutureLoadMetrics])
  ): mutable.HashMap[ActorVirtualIdentity, Long] = {
    val loads = new mutable.HashMap[ActorVirtualIdentity, Long]()
    for ((id, currLoad) <- cmd.sortLayer.workers.keys zip metrics._1) {
      loads(id) = currLoad.stashedBatches + currLoad.unprocessedQueueLength
//      detectSortSkewLogger.logInfo(
//        s"\tLOAD ${id} - ${currLoad.stashedBatches} stashed batches, ${currLoad.unprocessedQueueLength} internal queue, ${currLoad.totalPutInInternalQueue} total input"
//      )
    }
    metrics._2.foreach(replyFromNetComm => {
      for ((wId, futLoad) <- replyFromNetComm.dataToSend) {
        if (loads.contains(wId)) {
          loads(wId) = loads.getOrElse(wId, 0L) + futLoad
          // detectSortSkewLogger.logInfo(s"\tLOAD ${wId} - ${futLoad} going to arrive")
        }
      }
    })
    loads
  }

  private def aggregateAndPrintSentCount(
      totalSentPerSender: Seq[Map[ActorVirtualIdentity, Long]]
  ): Unit = {
    val aggregatedSentCount = new mutable.HashMap[ActorVirtualIdentity, Long]()
    totalSentPerSender.foreach(senderCount => {
      for ((rec, count) <- senderCount) {
        aggregatedSentCount(rec) = aggregatedSentCount.getOrElse(rec, 0L) + count
      }
    })
    detectSortSkewLogger.logInfo(s"\tTOTAL SENT TILL NOW ${aggregatedSentCount.mkString("\n\t\t")}")
  }

  registerHandler { (cmd: DetectSortSkew, sender) =>
    {
      if (previousCallFinished) {
        previousCallFinished = false
        startTimeForMetricColl = System.nanoTime()
        Future
          .join(
            getResultsAsFuture(cmd.sortLayer, QueryLoadMetrics()),
            getResultsAsFuture(cmd.prevLayer, QueryNextOpLoadMetrics())
          )
          .flatMap(metrics => {
            endTimeForMetricColl = System.nanoTime()
            detectSortSkewLogger.logInfo(
              s"\tThe metrics have been collected in ${(endTimeForMetricColl - startTimeForMetricColl) / 1e9d}s"
            )
            val loads = aggregateLoadMetrics(cmd, metrics)
            detectSortSkewLogger.logInfo(s"\tThe final loads map ${loads.mkString("\n\t\t")}")

            val skewedAndFreeWorkers = getSkewedAndFreeWorkers(loads)
            if (skewedAndFreeWorkers.size > 0) {
              startTimeForNotification = System.nanoTime()

              val futuresArr = new ArrayBuffer[Future[Unit]]()
              skewedAndFreeWorkers.foreach(sf => {
                detectSortSkewLogger.logInfo(
                  s"\tSkewed Worker:${sf._1}, Free Worker:${sf._2}, notification required:${sf._3}"
                )
                if (sf._3) { futuresArr.append(send(SendStateTranferNotification(sf._2), sf._1)) }
              })
              Future
                .collect(futuresArr)
                .flatMap(res => {
                  endTimeForNotification = System.nanoTime()
                  detectSortSkewLogger.logInfo(
                    s"\tState Transfer notification sent in ${(endTimeForNotification - startTimeForNotification) / 1e9d}s"
                  )

                  startTimeForNetChange = System.nanoTime()
                  getShareFlowResultsAsFuture(
                    cmd.prevLayer,
                    skewedAndFreeWorkers
                  ).map(seq => {
                    endTimeForNetChange = System.nanoTime()
                    aggregateAndPrintSentCount(seq)
                    detectSortSkewLogger.logInfo(
                      s"\tTHE NETWORK SHARE HAS HAPPENED in ${(endTimeForNetChange - startTimeForNetChange) / 1e9d}s"
                    )
                    previousCallFinished = true
                    CommandCompleted()
                  })
                })
            } else {
              val actualSkewedAndFreeGettingSkewedWorkers = isfreeGettingSkewed(loads)
              if (actualSkewedAndFreeGettingSkewedWorkers.size > 0) {
                actualSkewedAndFreeGettingSkewedWorkers.foreach(sf =>
                  detectSortSkewLogger.logInfo(
                    s"\tFree Worker Getting skewed:${sf._2}, Actual skewed Worker:${sf._1}"
                  )
                )

                startTimeForNetRollback = System.nanoTime()
                getRollbackFlowResultsAsFuture(
                  cmd.prevLayer,
                  actualSkewedAndFreeGettingSkewedWorkers
                ).map(seq => {
                  startTimeForNetRollback = System.nanoTime()
                  aggregateAndPrintSentCount(seq)
                  detectSortSkewLogger.logInfo(
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
