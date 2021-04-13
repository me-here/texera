package edu.uci.ics.texera.workflow.operators.source.scan.file

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig

abstract class FileScanSourceOpExecConfig(
    tag: OperatorIdentity,
    numWorkers: Int,
    totalBytes: Long
) extends OpExecConfig(tag) {

  def getOpExec(startOffset: Long, endOffset: Long): IOperatorExecutor

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          LayerIdentity(tag, "main"),
          i => {
            val startOffset: Long = totalBytes / numWorkers * i
            val endOffset: Long =
              if (i != numWorkers - 1) totalBytes / numWorkers * (i + 1) else totalBytes
            getOpExec(startOffset, endOffset)
          },
          numWorkers,
          UseAll(), // it's source operator
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
