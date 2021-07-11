package edu.uci.ics.texera.workflow.common.workflow

import com.typesafe.scalalogging.Logger
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.sink.CacheSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc

import scala.collection.mutable

//TODO: Use WorkflowResultService.
class WorkflowRewriter(
    workflowInfo: WorkflowInfo,
    operatorOutputCache: mutable.HashMap[String, mutable.MutableList[Tuple]],
    cachedOperators: mutable.HashMap[String, OperatorDescriptor],
    cacheSourceOperators: mutable.HashMap[String, CacheSourceOpDesc],
    cacheSinkOperators: mutable.HashMap[String, CacheSinkOpDesc]
) {
  private val logger = Logger(this.getClass.getName)

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
  private val rewrittenToCacheOperator = if (null != workflowInfo) {
    new mutable.HashSet[String]()
  } else {
    null
  }

  def rewrite: WorkflowInfo = {
    if (null == workflowInfo) {
      logger.info("Rewriting workflow null")
      return null
    }
    logger.info("Rewriting workflow {}", workflowInfo)
    checkCacheValidity()
    // Topological traversal
    workflowDAG.getSinkOperators.foreach(sinkOpID => {
      opIDQueue.enqueue(sinkOpID)
      newOperators += workflowDAG.getOperator(sinkOpID)
      addMatchingBreakpoints(sinkOpID)
    })
    while (opIDQueue.nonEmpty) {
      val opID: String = opIDQueue.dequeue()
      workflowDAG
        .getUpstream(opID)
        .foreach(upstreamOp => {
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
    workflowDAG
      .getDownstream(operatorID)
      .foreach(desc => {
        checkOperatorCacheValidity(desc.operatorID)
      })
  }

  private def invalidateCache(operatorID: String): Unit = {
    logger.debug("Operator {} cache invalidated.", operatorID)
    operatorOutputCache.remove(operatorID)
    cachedOperators.remove(operatorID)
    workflowDAG
      .getDownstream(operatorID)
      .foreach(desc => {
        invalidateCache(desc.operatorID)
      })
  }

  private def rewriteUpstreamOperator(opID: String, upstreamOp: OperatorDescriptor): Unit = {
    if (isCacheEnabled(upstreamOp)) {
      if (isCacheValid(upstreamOp)) {
        rewriteCachedOperator(upstreamOp)
      } else {
        rewriteToCacheOperator(opID, upstreamOp)
      }
    } else {
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
    addMatchingBreakpoints(upstreamOp.operatorID)
  }

  private def addMatchingBreakpoints(sinkOpID: String): Unit = {
    workflowInfo.breakpoints.foreach(breakpoint => {
      if (sinkOpID.equals(breakpoint.operatorID)) {
        newBreakpoints += breakpoint
      }
    })
  }

  private def rewriteToCacheOperator(opID: String, upstreamOp: OperatorDescriptor): Unit = {
    if (!rewrittenToCacheOperator.contains(upstreamOp.operatorID)) {
      logger.debug("Rewrite operator {}.", upstreamOp.operatorID)
      val toCacheOperator = generateCacheSinkOperator(upstreamOp)
      newOperators += toCacheOperator
      newLinks += generateToCacheLink(toCacheOperator, workflowDAG.getOperator(opID))
      rewrittenToCacheOperator.add(upstreamOp.operatorID)
    } else {
      logger.debug("Operator {} is already rewritten.", upstreamOp.operatorID)
    }
    rewriteNormalOperator(opID, upstreamOp)
  }

  private def rewriteCachedOperator(upstreamOp: OperatorDescriptor): Unit = {
    // Rewrite cached operator.
    val cachedOperator = getCacheSourceOperator(upstreamOp)
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
    if (!workflowInfo.operatorsToCache.contains(operator.operatorID)) {
      operatorOutputCache.remove(operator.operatorID)
      cachedOperators.remove(operator.operatorID)
      logger.debug("Operator {} cache not enabled.", operator)
      return false
    }
    logger.debug("Operator {} cache enabled.", operator)
    true
  }

  private def isCacheValid(operator: OperatorDescriptor): Boolean = {
    assert(isCacheEnabled(operator))
    if (cachedOperators.contains(operator.operatorID)) {
      if (getCachedOperator(operator).equals(operator) && !rewrittenToCacheOperator.contains(operator.operatorID)) {
        logger.debug("Operator {} cache valid.", operator)
        return true
      }
      logger.debug("Operator {} cache invalid.", operator)
    }
    logger.debug("Operator {} is never cached.", operator)
    false
  }

  private def getCachedOperator(operator: OperatorDescriptor): OperatorDescriptor = {
    assert(cachedOperators.contains(operator.operatorID))
    cachedOperators(operator.operatorID)
  }

  private def generateNewLinks(
      operator: OperatorDescriptor,
      upstreamOp: OperatorDescriptor
  ): mutable.MutableList[OperatorLink] = {
    val newLinks = mutable.MutableList[OperatorLink]()
    workflowDAG.jgraphtDag
      .edgesOf(upstreamOp.operatorID)
      .forEach(link => {
        val origin = OperatorPort(operator.operatorID, link.origin.portOrdinal)
        val newLink = OperatorLink(origin, link.destination)
        newLinks += newLink
      })
    newLinks
  }

  private def generateNewBreakpoints(
      newOperator: OperatorDescriptor,
      upstreamOp: OperatorDescriptor
  ): mutable.MutableList[BreakpointInfo] = {
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

  private def generateCacheSinkOperator(operator: OperatorDescriptor): CacheSinkOpDesc = {
    val outputCache = mutable.MutableList[Tuple]()
    cachedOperators.+=((operator.operatorID, operator))
    val cacheSinkOperator = new CacheSinkOpDesc(outputCache)
    cacheSinkOperators += ((operator.operatorID, cacheSinkOperator))
    val cacheSourceOperator = new CacheSourceOpDesc(outputCache)
    cacheSourceOperators += ((operator.operatorID, cacheSourceOperator))
    cacheSinkOperator
  }

  private def getCacheSourceOperator(operator: OperatorDescriptor): CacheSourceOpDesc = {
    val cacheSourceOperator = cacheSourceOperators(operator.operatorID)
    cacheSourceOperator.schema = cacheSinkOperators(operator.operatorID).schema
    cacheSourceOperator
  }

  private def generateToCacheLink(
      toCacheOperator: OperatorDescriptor,
      upstream: OperatorDescriptor
  ): OperatorLink = {
    //TODO: How to set the port ordinal?
    val origin: OperatorPort = OperatorPort(upstream.operatorID, 0)
    val destination: OperatorPort = OperatorPort(toCacheOperator.operatorID, 0)
    OperatorLink(origin, destination)
  }
}
