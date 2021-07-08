package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor

import scala.collection.mutable

//TODO: Support rule-based rewriting.
//TODO: Refactor.
//TODO: Add unit test.
class WorkflowRewriter(workflowInfo: WorkflowInfo) {

  private val workflowDAG: WorkflowDAG = if (workflowInfo != null) {
    new WorkflowDAG(workflowInfo)
  } else {
    null
  }
  private val newOperators = if (workflowInfo != null) {
    mutable.MutableList[OperatorDescriptor]()
  } else {
    null
  }
  private val newLinks = if (workflowInfo != null) {
    mutable.MutableList[OperatorLink]()
  } else {
    null
  }
  private val newBreakpoints = if (workflowInfo != null) {
    mutable.MutableList[BreakpointInfo]()
  } else {
    null
  }
  private val opIDQueue = if (workflowInfo != null) {
    new mutable.Queue[String]()
  } else {
    null
  }

  def rewrite: WorkflowInfo = {
    if (null == workflowInfo) {
      return null
    }
    checkCacheValidity()
    // Topological traversal
    workflowDAG.getSinkOperators.foreach(sinkOpID => {
      opIDQueue.enqueue(sinkOpID)
      newOperators += workflowDAG.getOperator(sinkOpID)
      workflowInfo.breakpoints.foreach(breakpoint => {
        addMatchingBreakpoint(sinkOpID, breakpoint)
      })
    })
    while (opIDQueue.nonEmpty) {
      val opID: String = opIDQueue.dequeue()
      workflowDAG.getUpstream(opID).foreach(upstreamOp => {
        rewriteUpstreamOperator(opID, upstreamOp)
      })
    }
    WorkflowInfo(newOperators, newLinks, newBreakpoints)
  }

  private def checkCacheValidity(): Unit = {
    val sourceOperators: List[String] = workflowDAG.getSourceOperators
    sourceOperators.foreach(operator => {
      checkOperatorCacheValidity(operator)
    })
  }

  private def checkOperatorCacheValidity(operatorID: String): Unit = {
    val desc = workflowDAG.getOperator(operatorID)
    if (isCacheEnabled(desc) && !isCacheValid(desc)) {
      invalidateCache(operatorID)
    }
    workflowDAG.getDownstream(operatorID).foreach(desc => {
      checkOperatorCacheValidity(desc.operatorID)
    })
  }

  private def invalidateCache(operatorID: String): Unit = {
    workflowDAG.getOperator(operatorID).isCacheEnabled = false
    workflowDAG.getDownstream(operatorID).foreach(desc => {
      invalidateCache(desc.operatorID)
    })
  }

  private def rewriteUpstreamOperator(opID: String, upstreamOp: OperatorDescriptor): Unit = {
    if (isCacheEnabled(upstreamOp)) {
      if (isCacheValid(upstreamOp)) {
        rewriteCachedOperator(upstreamOp)
      }
      else {
        rewriteToCacheOperator(opID, upstreamOp)
      }
    }
    else {
      rewriteNormalOperator(opID, upstreamOp)
    }
  }

  private def rewriteNormalOperator(opID: String, upstreamOp: OperatorDescriptor): Unit = {
    // Add the new link.
    newLinks += workflowDAG.jgraphtDag.getEdge(upstreamOp.operatorID, opID)
    // Remove the old link from the old DAG.
    workflowDAG.jgraphtDag.removeEdge(upstreamOp.operatorID, opID)
    // All outgoing neighbors of this upstream operator are handled.
    if (0.equals(workflowDAG.jgraphtDag.outDegreeOf(upstreamOp.operatorID))) {
      // Handle the incoming neighbors of this upstream operator.
      opIDQueue.enqueue(upstreamOp.operatorID)
      // Add the upstream operator.
      newOperators += upstreamOp
    }
    // Add the old breakpoints.
    workflowInfo.breakpoints.foreach(breakpoint => {
      addMatchingBreakpoint(upstreamOp.operatorID, breakpoint)
    })
  }

  private def addMatchingBreakpoint(sinkOpID: String, breakpoint: BreakpointInfo): Unit = {
    if (sinkOpID.equals(breakpoint.operatorID)) {
      newBreakpoints += breakpoint
    }
  }

  private def rewriteToCacheOperator(opID: String, upstreamOp: OperatorDescriptor): Unit = {
    // Rewrite To-cache operator.
    val toCacheOperator = generateToCacheOperator(upstreamOp)
    // Add the new operator.
    newOperators += toCacheOperator
    // Add the new links.
    newLinks += workflowDAG.jgraphtDag.getEdge(upstreamOp.operatorID, opID)
    // Add new links.
    newLinks += generateToCacheLink(toCacheOperator)
    // Remove the old link from the old DAG.
    workflowDAG.jgraphtDag.removeEdge(upstreamOp.operatorID, opID)
  }

  private def rewriteCachedOperator(upstreamOp: OperatorDescriptor): Unit = {
    // Rewrite cached operator.
    val cachedOperator = getCachedOperator(upstreamOp)
    //Add the new operator
    newOperators += cachedOperator
    // Add new links.
    generateNewLinks(cachedOperator, upstreamOp).foreach(newLink => {
      newLinks += newLink
    })
    // Add new breakpoints.
    generateNewBreakpoints(cachedOperator, upstreamOp).foreach(newBreakpoint => {
      newBreakpoints += newBreakpoint
    })
    // Remove the old operator and links from the old DAG.
    removeFromWorkflow(upstreamOp)
  }

  private def isCacheEnabled(operator: OperatorDescriptor): Boolean = {
    operator.isCacheEnabled
  }

  private def isCacheValid(operator: OperatorDescriptor): Boolean = {
    //TODO: Add cache map with cache invalidation strategy.
    false
  }

  private def getCachedOperator(operator: OperatorDescriptor): OperatorDescriptor = {
    //TODO: Map the original operator to the cached (source) operator.
    null
  }

  private def generateNewLinks(operator: OperatorDescriptor,
                               upstreamOp: OperatorDescriptor):
  mutable.MutableList[OperatorLink] = {
    val newLinks = mutable.MutableList[OperatorLink]()
    workflowDAG.jgraphtDag.edgesOf(upstreamOp.operatorID).forEach(link => {
      val origin = OperatorPort(operator.operatorID, link.origin.portOrdinal)
      val newLink = OperatorLink(origin, link.destination)
      newLinks += newLink
    })
    newLinks
  }

  private def generateNewBreakpoints(newOperator: OperatorDescriptor,
                                     upstreamOp: OperatorDescriptor):
  mutable.MutableList[BreakpointInfo] = {
    val breakpointInfoList = new mutable.MutableList[BreakpointInfo]()
    workflowInfo.breakpoints.foreach(info => {
      if (upstreamOp.operatorID.equals(info.operatorID)) {
        breakpointInfoList += BreakpointInfo(newOperator.operatorID, info.breakpoint)
      }
    })
    breakpointInfoList
  }

  private def removeFromWorkflow(operator: OperatorDescriptor): Unit = {
    workflowDAG.jgraphtDag.removeVertex(operator.operatorID)
  }

  private def generateToCacheOperator(operator: OperatorDescriptor): OperatorDescriptor = {
    null
  }

  private def generateToCacheLink(toCacheOperator: OperatorDescriptor): OperatorLink = {
    null
  }
}
