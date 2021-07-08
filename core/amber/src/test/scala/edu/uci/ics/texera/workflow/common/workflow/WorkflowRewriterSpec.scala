package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class WorkflowRewriterSpec extends AnyFlatSpec with BeforeAndAfter {
  var rewriter: WorkflowRewriter = _


  it should "return null" in {
    rewriter = new WorkflowRewriter(null)
    assert(rewriter.rewrite == null)
  }

  it should "return empty workflowInfo" in {
    val workflowInfo: WorkflowInfo = new WorkflowInfo(mutable.MutableList[OperatorDescriptor](),
      mutable.MutableList[OperatorLink](),
      mutable.MutableList[BreakpointInfo]())
    rewriter = new WorkflowRewriter(workflowInfo)
    assert(rewriter.rewrite.equals(workflowInfo))
  }
}
