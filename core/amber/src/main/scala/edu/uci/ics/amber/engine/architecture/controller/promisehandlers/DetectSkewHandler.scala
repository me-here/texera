package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.DetectSkewHandler.DetectSkew
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryLoadMetricsHandler.{
  CurrentLoadMetrics,
  QueryLoadMetrics
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryNextOpLoadMetricsHandler.{
  FutureLoadMetrics,
  QueryNextOpLoadMetrics
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DetectSkewHandler {
  final case class DetectSkew(joinLayer: WorkerLayer, probeLayer: WorkerLayer)
      extends ControlCommand[CommandCompleted]
}

// join-skew research related
trait DetectSkewHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: DetectSkew, sender) =>
    {
      // collect metrics from the skewed operator. assuming this is a single layer operator
      val workerMetricsFutures = new ArrayBuffer[Future[CurrentLoadMetrics]]()
      cmd.joinLayer.workers.keys.foreach(id => {
        workerMetricsFutures.append(send(QueryLoadMetrics(), id))
      })
      val futureOfWorkerMetrics = Future.collect(workerMetricsFutures.toList)

      // collect metrics from upstream operator of skewed operator. assuming it is a join case and contacting the probe side
      val inputOpMetricsFutures = new ArrayBuffer[Future[FutureLoadMetrics]]()
      cmd.probeLayer.workers.keys.foreach(id => {
        inputOpMetricsFutures.append(send(QueryNextOpLoadMetrics(), id))
      })
      val futureOfNextOpLoadMetrics = Future.collect(inputOpMetricsFutures.toList)

      // combining the two metrics
      val combinedFuture = Future.join(futureOfWorkerMetrics, futureOfNextOpLoadMetrics)
      val loads = new mutable.HashMap[ActorVirtualIdentity, Long]()
      combinedFuture.map(metrics => {
        for ((id, currLoad) <- cmd.joinLayer.workers.keys zip metrics._1) {
          loads(id) = currLoad.stashedBatches + currLoad.unprocessedQueueLength
          println(
            s"LOAD ${id} - ${currLoad.stashedBatches} stashed batches, ${currLoad.unprocessedQueueLength} internal queue"
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
        println(s"The final loads map ${loads.mkString("\n\t\t")}")
        CommandCompleted()
      })
    }
  }

}
