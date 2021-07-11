package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.sink.SimpleSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.csv.CSVScanSourceOpDesc
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class WorkflowRewriterSpec extends AnyFlatSpec with BeforeAndAfter {
  var rewriter: WorkflowRewriter = _

  it should "return null" in {
    rewriter = new WorkflowRewriter(null, null, null)
    assert(rewriter.rewrite == null)
  }

  it should "return empty workflowInfo" in {
    val workflowInfo: WorkflowInfo = new WorkflowInfo(
      mutable.MutableList[OperatorDescriptor](),
      mutable.MutableList[OperatorLink](),
      mutable.MutableList[BreakpointInfo]()
    )
    rewriter = new WorkflowRewriter(workflowInfo, null, null)
    assert(rewriter.rewrite.equals(workflowInfo))
  }

  it should "modify no operator" in {
    val operators = mutable.MutableList[OperatorDescriptor]()
    val links = mutable.MutableList[OperatorLink]()
    val breakpoints = mutable.MutableList[BreakpointInfo]()
    val sourceOpDesc = new CSVScanSourceOpDesc()
    val sinkOpDesc = new SimpleSinkOpDesc()
    operators += sourceOpDesc
    operators += sinkOpDesc
    val origin = OperatorPort(sourceOpDesc.operatorID, 0)
    val destination = OperatorPort(sinkOpDesc.operatorID, 0)
    links += OperatorLink(origin, destination)
    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
    workflowInfo.operatorsToCache = List[String]()
    rewriter = new WorkflowRewriter(
      workflowInfo,
      mutable.HashMap[String, mutable.MutableList[Tuple]](),
      mutable.HashMap[String, OperatorDescriptor]()
    )
    val rewrittenWorkflowInfo = rewriter.rewrite
    rewrittenWorkflowInfo.operators.foreach(operator => {
      assert(operators.contains(operator))
    })
  }
}
