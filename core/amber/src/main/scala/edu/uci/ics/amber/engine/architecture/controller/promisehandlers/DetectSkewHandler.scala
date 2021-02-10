package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.DetectSkewHandler.{
  DetectSkew,
  getSkewedAndFreeWorker,
  previousCallFinished,
  skewedWorkerToFreeWorker
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
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DetectSkewHandler {
  var previousCallFinished = true
  var skewedWorkerToFreeWorker = new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
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
      (skewedWorker, freeWorker)
//      (null, null)
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
      println(
        s"\tLOAD ${id} - ${currLoad.stashedBatches} stashed batches, ${currLoad.unprocessedQueueLength} internal queue"
      )
    }
    metrics._2.foreach(replyFromNetComm => {
      for ((wId, futLoad) <- replyFromNetComm.dataToSend) {
        if (loads.contains(wId)) {
          loads(wId) = loads.getOrElse(wId, 0L) + futLoad
          println(s"\tLOAD ${wId} - ${futLoad} going to arrive")
        }
      }
    })
    loads
  }

  registerHandler { (cmd: DetectSkew, sender) =>
    {
      if (previousCallFinished) {
        previousCallFinished = false
        Future
          .join(
            getResultsAsFuture(cmd.joinLayer, QueryLoadMetrics()),
            getResultsAsFuture(cmd.probeLayer, QueryNextOpLoadMetrics())
          )
          .flatMap(metrics => {
            val loads = aggregateLoadMetrics(cmd, metrics)
            println(s"The final loads map ${loads.mkString("\n\t\t")}")

            val skewedAndFreeWorkers = getSkewedAndFreeWorker(loads)
            if (skewedAndFreeWorkers._1 != null && skewedAndFreeWorkers._2 != null) {
              send(SendBuildTable(skewedAndFreeWorkers._2), skewedAndFreeWorkers._1).flatMap(
                res => {
                  println(
                    s"BUILD TABLE COPIED from ${skewedAndFreeWorkers._1} to ${skewedAndFreeWorkers._2}"
                  )
                  getResultsAsFuture(
                    cmd.probeLayer,
                    ShareFlow(skewedAndFreeWorkers._1, skewedAndFreeWorkers._2)
                  ).map(seq => {
                    println("THE NETWORK CHANGE HAS HAPPENED")
                    previousCallFinished = true
                    CommandCompleted()
                  })
                }
              )
            } else { Future { CommandCompleted() } }
          })
      } else { Future { CommandCompleted() } }
    }
  }

}
