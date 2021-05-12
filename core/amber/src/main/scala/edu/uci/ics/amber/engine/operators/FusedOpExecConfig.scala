package edu.uci.ics.amber.engine.operators
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.OneToOne
import edu.uci.ics.amber.engine.architecture.principal.OperatorStatistics
import edu.uci.ics.amber.engine.architecture.worker.FusedWorkerStatistics
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.WorkerState
import edu.uci.ics.amber.engine.common.{Constants, FusedOperatorExecutor}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, OperatorIdentity}

import scala.collection.mutable

class FusedOpExecConfig(operators:OpExecConfig*) extends OpExecConfig(OperatorIdentity(operators.head.id.workflow, operators.map(_.id.operator).mkString("+"))) {

  assert(operators.size > 1)
  assert(operators.forall(!_.requiredShuffle))
  assert(operators.forall(!_.isInstanceOf[SinkOpExecConfig]))
  assert(operators.forall(_.topology.links.forall(_.isInstanceOf[OneToOne])))
  private val firstLayer = operators.head.topology.layers.head
  assert(operators.forall(_.topology.layers.forall(_.numWorkers == firstLayer.numWorkers)))

  private val fusedOperatorStats = mutable.HashMap[ActorVirtualIdentity, Array[(Long,Long)]]()

  override lazy val topology: Topology = new Topology(
    Array(
      new WorkerLayer(
        LayerIdentity(id, "fused"),
        x => new FusedOperatorExecutor(operators.flatMap(_.topology.layers.map(_.metadata(x))):_*),
        firstLayer.numWorkers,
        firstLayer.deploymentFilter,
        firstLayer.deployStrategy
      )
    ),
    Array()
  )

  def setOperatorStatsForWorker(worker:ActorVirtualIdentity, stats:FusedWorkerStatistics): Unit ={
    topology.layers.head.getWorkerInfo(worker).state = stats.workerState
    fusedOperatorStats(worker) = stats.rowCounts
  }

  def getFusedOperatorStats:Iterable[(String, OperatorStatistics)]={
    val stat = getOperatorStatistics
    if(fusedOperatorStats.isEmpty){
      operators.indices.map(i => {
        (operators(i).id.operator, OperatorStatistics(stat.operatorState, 0L, 0L, None))
      })
    }else{
      val counter = fusedOperatorStats.values.reduceLeft((a,b) => a.zip(b).map{
        case (x,y) => (x._1+y._1, x._2+y._2)
      })
      operators.indices.map(i => {
        (operators(i).id.operator, OperatorStatistics(stat.operatorState, counter(i)._1,counter(i)._2, None))
      })
    }
  }

  override def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity] = {
    //hack: skip for now
    Array.empty
  }
}
