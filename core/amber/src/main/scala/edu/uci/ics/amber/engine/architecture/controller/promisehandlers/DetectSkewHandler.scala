package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.DetectSkewHandler.{
  DetectSkew,
  convertToFirstPhaseCallFinished,
  convertToSecondPhaseCallFinished,
  detectSkewLogger,
  endTimeForBuildRepl,
  endTimeForMetricColl,
  endTimeForNetChange,
  endTimeForNetChangeForSecondPhase,
  getSkewedAndFreeWorkersEligibleForFirstPhase,
  getSkewedAndFreeWorkersEligibleForSecondPhase,
  isfreeGettingSkewed,
  previousCallFinished,
  skewedToFreeWorkerFirstPhase,
  skewedToFreeWorkerHistory,
  startTimeForBuildRepl,
  startTimeForMetricColl,
  startTimeForNetChange,
  startTimeForNetChangeForSecondPhase,
  startTimeForNetRollback,
  stopMitigationCallFinished,
  workerToTotalLoadHistory
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryLoadMetricsHandler.{
  CurrentLoadMetrics,
  QueryLoadMetrics
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryNextOpLoadMetricsHandler.{
  FutureLoadMetrics,
  QueryNextOpLoadMetrics,
  WorkloadHistory
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.RollbackFlowHandler.RollbackFlow
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SendBuildTableHandler.SendBuildTable
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShareFlowHandler.ShareFlow
import edu.uci.ics.amber.engine.common.AmberUtils.sampleMeanError
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
  var convertToFirstPhaseCallFinished = true
  var convertToSecondPhaseCallFinished = true
  var stopMitigationCallFinished = true
  var startTimeForMetricColl: Long = _
  var endTimeForMetricColl: Long = _
  var startTimeForBuildRepl: Long = _
  var endTimeForBuildRepl: Long = _
  var startTimeForNetChange: Long = _
  var endTimeForNetChange: Long = _
  var startTimeForNetChangeForSecondPhase: Long = _
  var endTimeForNetChangeForSecondPhase: Long = _
  var startTimeForNetRollback: Long = _
  var endTimeForNetRollback: Long = _
  var detectSkewLogger: WorkflowLogger = new WorkflowLogger("DetectSkewHandler")

  var skewedToFreeWorkerFirstPhase =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  var skewedToFreeWorkerSecondPhase =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  var skewedToFreeWorkerHistory = new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  // worker to worker current input size
  var workerToLoadHistory = new mutable.HashMap[ActorVirtualIdentity, ListBuffer[Long]]()
  // (prevWorker, (worker, array of load per 1000 tuples for worker as in prevWorker))
  var workerToTotalLoadHistory =
    new mutable.HashMap[ActorVirtualIdentity, mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[
      Long
    ]]]()
  val historyLimit = 1

  final case class DetectSkew(joinLayer: WorkerLayer, probeLayer: WorkerLayer)
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

  /**
    * worker is eligible for first phase if no mitigation has happened till now or it is in second phase right now.
    * @param worker
    * @return
    */
  def isEligibleForSkewedAndForFirstPhase(worker: ActorVirtualIdentity): Boolean = {
    !skewedToFreeWorkerFirstPhase.keySet.contains(
      worker
    ) && !skewedToFreeWorkerFirstPhase.values.toList.contains(
      worker
    ) && !skewedToFreeWorkerSecondPhase.values.toList.contains(worker)
  }

  /**
    * worker is eligible for free if it is being used in neither of the phases.
    * @param worker
    * @return
    */
  def isEligibleForFree(worker: ActorVirtualIdentity): Boolean = {
    !skewedToFreeWorkerFirstPhase.keySet.contains(
      worker
    ) && !skewedToFreeWorkerFirstPhase.values.toList.contains(
      worker
    ) && !skewedToFreeWorkerSecondPhase.keySet.contains(
      worker
    ) && !skewedToFreeWorkerSecondPhase.values.toList.contains(worker)
  }

  def passSkewTest(
      skewedWorkerCand: ActorVirtualIdentity,
      freeWorkerCand: ActorVirtualIdentity,
      threshold: Double
  ): Boolean = {
    var isSkewed = true
    val skewedHist = workerToLoadHistory(skewedWorkerCand)
    val freeHist = workerToLoadHistory(freeWorkerCand)
    assert(skewedHist.size == freeHist.size)
    for (j <- 0 to skewedHist.size - 1) {
      if (skewedHist(j) < 100 || skewedHist(j) < threshold + freeHist(j)) {
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
    val freeWorkersInFirstPhase = skewedToFreeWorkerFirstPhase.values.toList
    val freeWorkersInSecondPhase = skewedToFreeWorkerSecondPhase.values.toList
    for (i <- 0 to sortedWorkers.size - 1) {
      if (
        freeWorkersInFirstPhase
          .contains(sortedWorkers(i)) || freeWorkersInSecondPhase.contains(sortedWorkers(i))
      ) {
        var actualSkewedWorker: ActorVirtualIdentity = null
        skewedToFreeWorkerFirstPhase.keys.foreach(sw => {
          if (skewedToFreeWorkerFirstPhase(sw) == sortedWorkers(i)) { actualSkewedWorker = sw }
        })
        if (actualSkewedWorker == null) {
          skewedToFreeWorkerSecondPhase.keys.foreach(sw => {
            if (skewedToFreeWorkerSecondPhase(sw) == sortedWorkers(i)) { actualSkewedWorker = sw }
          })
        }
        assert(actualSkewedWorker != null)

        if (!Constants.onlyDetectSkew && passSkewTest(sortedWorkers(i), actualSkewedWorker, 50)) {
          ret.append((actualSkewedWorker, sortedWorkers(i)))
        }
      }
    }
    ret
  }

  // return value is array of (skewedWorker, freeWorker, whether state replication has to be done)
  def getSkewedAndFreeWorkersEligibleForFirstPhase(
      loads: mutable.HashMap[ActorVirtualIdentity, Long]
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)] = {
    updateLoadHistory(loads)
    val ret = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)]()
    // Get workers in increasing load
    val sortedWorkers = loads.keys.toList.sortBy(loads(_))

    for (i <- sortedWorkers.size - 1 to 0 by -1) {
      if (isEligibleForSkewedAndForFirstPhase(sortedWorkers(i))) {
        // worker has been previously paired with some worker and that worker will be used again.
        // Also if the worker is in second phase, it will be put back in the first phase
        if (
          skewedToFreeWorkerHistory.keySet.contains(sortedWorkers(i)) && passSkewTest(
            sortedWorkers(i),
            skewedToFreeWorkerHistory(sortedWorkers(i)),
            100
          )
        ) {
          ret.append((sortedWorkers(i), skewedToFreeWorkerHistory(sortedWorkers(i)), false))
          skewedToFreeWorkerFirstPhase(sortedWorkers(i)) = skewedToFreeWorkerHistory(
            sortedWorkers(i)
          )
          skewedToFreeWorkerSecondPhase.remove(sortedWorkers(i))
        } else if (i > 0) {
          breakable {
            for (j <- 0 to i - 1) {
              if (
                isEligibleForFree(sortedWorkers(j)) && passSkewTest(
                  sortedWorkers(i),
                  sortedWorkers(j),
                  100
                )
              ) {
                ret.append((sortedWorkers(i), sortedWorkers(j), true))
                skewedToFreeWorkerFirstPhase(sortedWorkers(i)) = sortedWorkers(j)
                skewedToFreeWorkerHistory(sortedWorkers(i)) = sortedWorkers(j)
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

  // return value is array of (skewedWorker, freeWorker,  # tuples to redirect, out of total tuples)
  def getSkewedAndFreeWorkersEligibleForSecondPhase(
      loads: mutable.HashMap[ActorVirtualIdentity, Long]
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Long, Long)] = {
    val ret = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Long, Long)]()
    skewedToFreeWorkerFirstPhase.keys.foreach(skewedWorker => {
      if (loads(skewedWorker) <= loads(skewedToFreeWorkerFirstPhase(skewedWorker))) {
        ret.append((skewedWorker, skewedToFreeWorkerFirstPhase(skewedWorker), 1L, 4L))
      }
    })
    ret
  }

}

// join-skew research related
trait DetectSkewHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  /**
    * Sends a control to a layer of workers and returns the list of results as future
    * @param workerLayer
    * @param message
    * @tparam T
    * @return
    */
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

  /**
    * Sends `ShareFlow` control message to each worker in `workerLayer`. The message says that flow has to be shared
    * between skewed and free workers in `skewedAndFreeWorkersList`.
    * @param workerLayer
    * @param skewedAndFreeWorkersList
    * @tparam T
    * @return
    */
  private def getShareFlowFirstPhaseResultsAsFuture[T](
      workerLayer: WorkerLayer,
      skewedAndFreeWorkersList: ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)]
  ): Future[Seq[Map[ActorVirtualIdentity, Long]]] = {
    val futuresArr = new ArrayBuffer[Future[Map[ActorVirtualIdentity, Long]]]()
    skewedAndFreeWorkersList.foreach(sf => {
      workerLayer.workers.keys.foreach(id => {
        futuresArr.append(
          send(ShareFlow(sf._1, sf._2, 1, 2), id)
        )
      })
    })
    Future.collect(futuresArr)
  }

  private def getShareFlowSecondPhaseResultsAsFuture[T](
      workerLayer: WorkerLayer,
      skewedAndFreeWorkersList: ArrayBuffer[
        (ActorVirtualIdentity, ActorVirtualIdentity, Long, Long)
      ]
  ): Future[Seq[Map[ActorVirtualIdentity, Long]]] = {
    val futuresArr = new ArrayBuffer[Future[Map[ActorVirtualIdentity, Long]]]()
    skewedAndFreeWorkersList.foreach(sf => {
      workerLayer.workers.keys.foreach(id => {
        futuresArr.append(
          send(ShareFlow(sf._1, sf._2, sf._3, sf._4), id)
        )
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
      cmd: DetectSkew,
      metrics: (Seq[CurrentLoadMetrics], Seq[(FutureLoadMetrics, WorkloadHistory)])
  ): mutable.HashMap[ActorVirtualIdentity, Long] = {
    val loads = new mutable.HashMap[ActorVirtualIdentity, Long]()
    for ((id, currLoad) <- cmd.joinLayer.workers.keys zip metrics._1) {
      loads(id) = currLoad.stashedBatches + currLoad.unprocessedQueueLength
//      detectSkewLogger.logInfo(
//        s"\tLOAD ${id} - ${currLoad.stashedBatches} stashed batches, ${currLoad.unprocessedQueueLength} internal queue, ${currLoad.totalPutInInternalQueue} total input"
//      )
    }
    metrics._2.foreach(replyFromNetComm => {
      for ((wId, futLoad) <- replyFromNetComm._1.dataToSend) {
        if (loads.contains(wId)) {
          loads(wId) = loads.getOrElse(wId, 0L) + futLoad
          // detectSkewLogger.logInfo(s"\tLOAD ${wId} - ${futLoad} going to arrive")
        }
      }
    })
    for ((prevWId, replyFromPrevId) <- cmd.probeLayer.workers.keys zip metrics._2) {
      var prevWorkerMap = workerToTotalLoadHistory.getOrElse(
        prevWId,
        new mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]()
      )
      for ((wid, loadHistory) <- replyFromPrevId._2.history) {
        var existingHistoryForWid = prevWorkerMap.getOrElse(wid, new ArrayBuffer[Long]())
        // clean up to save memory
        if (existingHistoryForWid.size >= 500) {
          existingHistoryForWid = new ArrayBuffer[Long]()
        }
        existingHistoryForWid.appendAll(loadHistory)
        if (wid.toString().contains("main)[3]")) {
          print(s"\tLOADS FROM ${prevWId} are : ")
          var stop = existingHistoryForWid.size - 11
          if (stop < 0) { stop = 0 }
          for (i <- existingHistoryForWid.size - 1 to stop by -1) {
            print(existingHistoryForWid(i) + ", ")
          }
          print(s"Standard error is ${sampleMeanError(existingHistoryForWid)}")
          println()
        }
        prevWorkerMap(wid) = existingHistoryForWid
      }
      workerToTotalLoadHistory(prevWId) = prevWorkerMap
    }
    loads
  }

  /**
    * Prints the total # of tuples sent to workers in the skewed operator till now.
    * @param totalSentPerSender
    */
  private def aggregateAndPrintSentCount(
      totalSentPerSender: Seq[Map[ActorVirtualIdentity, Long]]
  ): Unit = {
    val aggregatedSentCount = new mutable.HashMap[ActorVirtualIdentity, Long]()
    totalSentPerSender.foreach(senderCount => {
      for ((rec, count) <- senderCount) {
        aggregatedSentCount(rec) = aggregatedSentCount.getOrElse(rec, 0L) + count
      }
    })
    detectSkewLogger.logInfo(s"\tTOTAL SENT TILL NOW ${aggregatedSentCount.mkString("\n\t\t")}")
  }

  registerHandler { (cmd: DetectSkew, sender) =>
    {
      if (
        previousCallFinished && convertToFirstPhaseCallFinished &&
        convertToSecondPhaseCallFinished && stopMitigationCallFinished
      ) {
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

            // Start first phase for workers getting skewed for first time or in second phase
            val skewedAndFreeWorkersForFirstPhase =
              getSkewedAndFreeWorkersEligibleForFirstPhase(loads)
            if (skewedAndFreeWorkersForFirstPhase.size > 0) {
              convertToFirstPhaseCallFinished = false
              startTimeForBuildRepl = System.nanoTime()

              val futuresArr = new ArrayBuffer[Future[Seq[Unit]]]()
              skewedAndFreeWorkersForFirstPhase.foreach(sf => {
                detectSkewLogger.logInfo(
                  s"\tSkewed Worker:${sf._1}, Free Worker:${sf._2}, build replication:${sf._3}"
                )
                if (sf._3) { futuresArr.append(send(SendBuildTable(sf._2), sf._1)) }
              })
              Future
                .collect(futuresArr)
                .flatMap(res => {
                  endTimeForBuildRepl = System.nanoTime()
                  detectSkewLogger.logInfo(
                    s"\tBUILD TABLES COPIED in ${(endTimeForBuildRepl - startTimeForBuildRepl) / 1e9d}s"
                  )

                  startTimeForNetChange = System.nanoTime()
                  getShareFlowFirstPhaseResultsAsFuture(
                    cmd.probeLayer,
                    skewedAndFreeWorkersForFirstPhase
                  ).map(seq => {
                    endTimeForNetChange = System.nanoTime()
                    aggregateAndPrintSentCount(seq)
                    detectSkewLogger.logInfo(
                      s"\tTHE NETWORK SHARE HAS HAPPENED in ${(endTimeForNetChange - startTimeForNetChange) / 1e9d}s"
                    )
                    convertToFirstPhaseCallFinished = true
                  })
                })
            }

            // check the pairs in first phase and see if they have to be shifted to second phase
            val skewedAndFreeWorkersForSecondPhase =
              getSkewedAndFreeWorkersEligibleForSecondPhase(loads)
            if (skewedAndFreeWorkersForSecondPhase.size > 0) {
              convertToSecondPhaseCallFinished = false
              skewedAndFreeWorkersForSecondPhase.foreach(sf =>
                detectSkewLogger.logInfo(
                  s"\tSkewed Worker:${sf._1}, Free Worker:${sf._2} moving to second phase"
                )
              )
              startTimeForNetChangeForSecondPhase = System.nanoTime()
              getShareFlowSecondPhaseResultsAsFuture(
                cmd.probeLayer,
                skewedAndFreeWorkersForSecondPhase
              ).map(seq => {
                endTimeForNetChangeForSecondPhase = System.nanoTime()
                detectSkewLogger.logInfo(
                  s"\tTHE SECOND PHASE NETWORK SHARE HAS HAPPENED in ${(endTimeForNetChangeForSecondPhase - startTimeForNetChangeForSecondPhase) / 1e9d}s"
                )
                convertToSecondPhaseCallFinished = true
              })
            }

            // stop mitigation for worker pairs where mitigation is causing free worker to become skewed
            val actualSkewedAndFreeGettingSkewedWorkers = isfreeGettingSkewed(loads)
            if (actualSkewedAndFreeGettingSkewedWorkers.size > 0) {
              stopMitigationCallFinished = false
              actualSkewedAndFreeGettingSkewedWorkers.foreach(sf =>
                detectSkewLogger.logInfo(
                  s"\tFree Worker Getting skewed:${sf._2}, Actual skewed Worker:${sf._1}"
                )
              )

              startTimeForNetRollback = System.nanoTime()
              getRollbackFlowResultsAsFuture(
                cmd.probeLayer,
                actualSkewedAndFreeGettingSkewedWorkers
              ).map(seq => {
                startTimeForNetRollback = System.nanoTime()
                aggregateAndPrintSentCount(seq)
                detectSkewLogger.logInfo(
                  s"\tTHE NETWORK ROLLBACK HAS HAPPENED in ${(endTimeForNetChange - startTimeForNetChange) / 1e9d}s"
                )
                stopMitigationCallFinished = true
              })
            }

            previousCallFinished = true
            Future { CommandCompleted() }
          })
      } else { Future { CommandCompleted() } }
    }
  }

}
