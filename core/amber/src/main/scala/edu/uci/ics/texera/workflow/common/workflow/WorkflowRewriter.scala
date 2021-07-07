package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor

import scala.collection.mutable

class WorkflowRewriter {
  var workflowInfo: WorkflowInfo = _

  def rewrite: WorkflowInfo = {
    val workflowDAG = new WorkflowDAG(workflowInfo)
    val sinkOperatorIDs = workflowDAG.getSinkOperators
    val newOperators = mutable.MutableList[OperatorDescriptor]()
    val newLinks = mutable.MutableList[OperatorLink]()
    val newBreakpoints = mutable.MutableList[BreakpointInfo]()

    //    Topological traversal
    val opIDQueue = new mutable.Queue[String]()
    sinkOperatorIDs.foreach(ID => opIDQueue.enqueue(ID))
    while (opIDQueue.nonEmpty) {
      val opID: String = opIDQueue.dequeue()
      workflowDAG.getUpstream(opID).foreach(upstreamOp => {
        workflowDAG.jgraphtDag.outDegreeOf(upstreamOp.operatorID) -= 1
        if (isCacheEnabled(upstreamOp)) {
          if (isCacheValid(upstreamOp)) {
            // Rewrite cached operator.
            val newOperator = getCachedOperator(upstreamOp)
            generateNewLinks(newOperator, workflowDAG).foreach(newLink => {
              newLinks += newLink
            })
            generateNewBreakpoints(newOperator, workflowDAG).foreach(newBreakpoint => {
              newBreakpoints += newBreakpoint
            })
            newOperators += newOperator
            removeFromWorkflow(upstreamOp, workflowDAG)
          }
          else {
            val cacheOperator = generateCacheOperator(upstreamOp, workflowDAG)
            val cacheLink = generateCacheLink(cacheOperator, upstreamOp)
            newOperators += cacheOperator
            newLinks += cacheLink
            newOperators += upstreamOp
            newLinks += workflowDAG.jgraphtDag.getEdge(upstreamOp.operatorID, opID)
          }
        }
        else {
          newLinks += workflowDAG.jgraphtDag.getEdge(upstreamOp.operatorID, opID)
          if (workflowDAG.jgraphtDag.outDegreeOf(upstreamOp.operatorID).equals(0)) {
            opIDQueue.enqueue(upstreamOp.operatorID)
            newOperators += upstreamOp
          }
        }
      })
    }
    WorkflowInfo(newOperators, newLinks, newBreakpoints)
  }

  private def isCacheEnabled(operator: OperatorDescriptor): Boolean = {
    operator.isCacheEnabled
  }

  private def isCacheValid(operator: OperatorDescriptor): Boolean = {
    false
  }

  private def getCachedOperator(operator: OperatorDescriptor): OperatorDescriptor = {
    null
  }

  private def generateNewLinks(operator: OperatorDescriptor,
                               workflowDAG: WorkflowDAG): mutable.MutableList[OperatorLink] = {
    null
  }

  private def generateNewBreakpoints(operator: OperatorDescriptor,
                                     workflowDAG: WorkflowDAG):
  mutable.MutableList[BreakpointInfo] = {
    null
  }

  private def removeFromWorkflow(operator: OperatorDescriptor, workflowDAG: WorkflowDAG): Unit = {
    workflowDAG.jgraphtDag.removeVertex(operator.operatorID)
  }

  private def generateCacheOperator(operator: OperatorDescriptor,
                                    workflowDAG: WorkflowDAG): OperatorDescriptor = {
    null
  }

  private def generateCacheLink(cacheOperator: OperatorDescriptor,
                                originalOperator: OperatorDescriptor): OperatorLink = {
    null
  }
}
