package edu.uci.ics.texera.workflow.operators.sink.file

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.ForceLocal
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RandomDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.SinkOpExecConfig
import edu.uci.ics.texera.workflow.common.WorkflowContext
import org.jooq.types.UInteger

class FileSinkOpExecConfig(
    val tag: OperatorIdentity,
    val fileName: String,
    val fileType: ResultFileType,
    val userId: UInteger
) extends SinkOpExecConfig(tag) {
  override lazy val topology = new Topology(
    Array(
      new WorkerLayer(
        LayerIdentity(tag, "main"),
        _ => new FileSinkOpExec(fileName, fileType, userId),
        1,
        ForceLocal(),
        RandomDeployment()
      )
    ),
    Array()
  )

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
