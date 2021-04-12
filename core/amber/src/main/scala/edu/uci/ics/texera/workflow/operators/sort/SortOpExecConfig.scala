package edu.uci.ics.texera.workflow.operators.sort

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.{
  RandomDeployment,
  RoundRobinDeployment
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.AllToOne
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class SortOpExecConfig(
    id: OperatorIdentity,
    val sortAttributeName: String
) extends OpExecConfig(id) {

  override lazy val topology: Topology = {
    var partialLayer = new WorkerLayer(
      LayerIdentity(id, "localSort"),
      i => new SortOpLocalExec(sortAttributeName, Constants.lowerLimit, Constants.upperLimit, i),
      Constants.defaultNumWorkers,
      UseAll(),
      RoundRobinDeployment()
    )
    var finalLayer = new WorkerLayer(
      LayerIdentity(id, "globalSort"),
      _ =>
        new SortOpFinalExec(
          sortAttributeName,
          Constants.lowerLimit,
          Constants.upperLimit,
          Constants.defaultNumWorkers
        ),
      1,
      UseAll(),
      RandomDeployment()
    )

    new Topology(
      Array(
        partialLayer,
        finalLayer
      ),
      Array(
        new AllToOne(partialLayer, finalLayer, Constants.defaultBatchSize)
      )
    )
  }

  override def requiredShuffle: Boolean = true

  override def requiredRangePartition: Boolean = true

  def getShuffleKey(layer: LayerIdentity): ITuple => String = { t: ITuple =>
    t.asInstanceOf[Tuple].getField(sortAttributeName).asInstanceOf[String]
  }

  override def getShuffleHashFunction(layer: LayerIdentity): ITuple => Int = { t: ITuple =>
    {
      val fieldVal: Float = t.asInstanceOf[Tuple].getField(sortAttributeName).asInstanceOf[Float]
      val jump: Int =
        ((Constants.upperLimit - Constants.lowerLimit) / Constants.defaultNumWorkers).toInt + 1
      var idx: Int = 0
      while (fieldVal >= jump * idx) {
        idx = idx + 1
      }
      idx - 1
    }
  }

  override def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
