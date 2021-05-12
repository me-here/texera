package edu.uci.ics.texera.workflow.common.workflow

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.linksemantics.OneToOne
import edu.uci.ics.amber.engine.common.{FusedOperatorExecutor, ISourceOperatorExecutor}
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.{FusedOpExecConfig, OpExecConfig}
import edu.uci.ics.texera.workflow.common.{ConstraintViolation, WorkflowContext}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}

import scala.collection.mutable

class WorkflowCompiler(val workflowInfo: WorkflowInfo, val context: WorkflowContext) {

  init()

  def init(): Unit = {
    this.workflowInfo.operators.foreach(initOperator)
  }

  def initOperator(operator: OperatorDescriptor): Unit = {
    operator.setContext(context)
  }

  def validate: Map[String, Set[ConstraintViolation]] =
    this.workflowInfo.operators
      .map(o => {
        o.operatorID -> {
          o.validate().toSet
        }
      })
      .toMap
      .filter(pair => pair._2.nonEmpty)

  def amberWorkflow: Workflow = {
    val amberOperators: mutable.Map[OperatorIdentity, OpExecConfig] = mutable.Map()
    workflowInfo.operators.foreach(o => {
      val amberOperator: OpExecConfig = o.operatorExecutor
      amberOperators.put(amberOperator.id, amberOperator)
    })

    val outLinks: mutable.Map[OperatorIdentity, mutable.Set[OperatorIdentity]] = mutable.Map()
    workflowInfo.links.foreach(link => {
      val origin = OperatorIdentity(this.context.jobID, link.origin.operatorID)
      val dest = OperatorIdentity(this.context.jobID, link.destination.operatorID)
      val destSet = outLinks.getOrElse(origin, mutable.Set())
      destSet.add(dest)
      outLinks.update(origin, destSet)
      val layerLink = LinkIdentity(
        amberOperators(origin).topology.layers.last.id,
        amberOperators(dest).topology.layers.head.id
      )
      amberOperators(dest).setInputToOrdinalMapping(layerLink, link.destination.portOrdinal)
    })

    //operator fusion:
    val allDests = outLinks.values.flatten.toSet
    val frontier = amberOperators.filter(x => !allDests.contains(x._1))
    while(frontier.nonEmpty){
      var current = frontier.head
      frontier.remove(current._1)
      //try to fuse:
      val result = dfsFusion(amberOperators, current, outLinks)
      if(result.length > 1){
        // create fused operator
        try{
          val fused = new FusedOpExecConfig(result.map(amberOperators.apply):_*)
          //remove all operators which are being fused
          result.foreach(amberOperators.remove)
          //modify out link
          if(outLinks.contains(result.last)){
            outLinks(fused.id) = outLinks(result.last)
          }
          result.foreach(outLinks.remove)
          //modify in link
          outLinks.foreach{
            case (k, v) if v.contains(result.head) => {
              v.remove(result.head)
              v.add(fused.id)
            }
            case other => //skip
          }
          //add fused operator
          amberOperators.put(fused.id, fused)
          current = (fused.id, fused)
        }catch{
          case e:Exception =>
          // failed
        }
      }
      if(outLinks.contains(current._1)) {
        outLinks(current._1).foreach(x => frontier
        .put(x, amberOperators(x)))
      }
    }

    val outLinksMutableValue: mutable.Map[OperatorIdentity, Set[OperatorIdentity]] =
      mutable.Map()
    outLinks.foreach(entry => {
      outLinksMutableValue.update(entry._1, entry._2.toSet)
    })

    new Workflow(amberOperators, outLinksMutableValue)
  }

  private def dfsFusion(amberOperators:mutable.Map[OperatorIdentity, OpExecConfig], current:(OperatorIdentity, OpExecConfig), links:mutable.Map[OperatorIdentity, mutable.Set[OperatorIdentity]]):Array[OperatorIdentity] = {
    if(!links.contains(current._1) || links(current._1).size != 1){
      Array.empty
    }else if(current._2.requiredShuffle){
      Array.empty
    }else if(current._2.topology.links.exists(!_.isInstanceOf[OneToOne])) {
      Array.empty
    }else{
      val opKey = links(current._1).head
      val opExecConfig = amberOperators(opKey)
      Array(current._1)++dfsFusion(amberOperators, (opKey, opExecConfig), links)
    }
  }

  def initializeBreakpoint(controller: ActorRef): Unit = {
    for (pair <- this.workflowInfo.breakpoints) {
      addBreakpoint(controller, pair.operatorID, pair.breakpoint)
    }
  }

  def addBreakpoint(
      controller: ActorRef,
      operatorID: String,
      breakpoint: Breakpoint
  ): Unit = {
    val breakpointID = "breakpoint-" + operatorID
    breakpoint match {
      case conditionBp: ConditionBreakpoint =>
        val column = conditionBp.column
        val predicate: Tuple => Boolean = conditionBp.condition match {
          case BreakpointCondition.EQ =>
            tuple => {
              tuple.getField(column).toString.trim == conditionBp.value
            }
          case BreakpointCondition.LT =>
            tuple => tuple.getField(column).toString.trim < conditionBp.value
          case BreakpointCondition.LE =>
            tuple => tuple.getField(column).toString.trim <= conditionBp.value
          case BreakpointCondition.GT =>
            tuple => tuple.getField(column).toString.trim > conditionBp.value
          case BreakpointCondition.GE =>
            tuple => tuple.getField(column).toString.trim >= conditionBp.value
          case BreakpointCondition.NE =>
            tuple => tuple.getField(column).toString.trim != conditionBp.value
          case BreakpointCondition.CONTAINS =>
            tuple => tuple.getField(column).toString.trim.contains(conditionBp.value)
          case BreakpointCondition.NOT_CONTAINS =>
            tuple => !tuple.getField(column).toString.trim.contains(conditionBp.value)
        }
      //TODO: add new handling logic here
//        controller ! PassBreakpointTo(
//          operatorID,
//          new ConditionalGlobalBreakpoint(
//            breakpointID,
//            tuple => {
//              val texeraTuple = tuple.asInstanceOf[Tuple]
//              predicate.apply(texeraTuple)
//            }
//          )
//        )
      case countBp: CountBreakpoint =>
//        controller ! PassBreakpointTo(
//          operatorID,
//          new CountGlobalBreakpoint("breakpointID", countBp.count)
//        )
    }
  }

  def propagateWorkflowSchema(): Map[OperatorDescriptor, List[Option[Schema]]] = {
    // construct the edu.uci.ics.texera.workflow DAG object using jGraphT
    val workflowDag =
      new DirectedAcyclicGraph[OperatorDescriptor, DefaultEdge](classOf[DefaultEdge])
    this.workflowInfo.operators.foreach(op => workflowDag.addVertex(op))
    this.workflowInfo.links.foreach(link => {
      val origin = workflowInfo.operators.find(op => op.operatorID == link.origin.operatorID).get
      val dest = workflowInfo.operators.find(op => op.operatorID == link.destination.operatorID).get
      workflowDag.addEdge(origin, dest)
    })

    // a map from an operator to the list of its input schema
    val inputSchemaMap =
      new mutable.HashMap[OperatorDescriptor, mutable.MutableList[Option[Schema]]]()
        .withDefault(op => mutable.MutableList.fill(op.operatorInfo.inputPorts.size)(Option.empty))

    // propagate output schema following topological order
    // TODO: introduce the concept of port in TexeraOperatorDescriptor and propagate schema according to port
    val topologicalOrderIterator = workflowDag.iterator()
    topologicalOrderIterator.forEachRemaining(op => {
      // infer output schema of this operator based on its input schema
      val outputSchema: Option[Schema] = {
        if (op.isInstanceOf[SourceOperatorDescriptor]) {
          // op is a source operator, ask for it output schema
          Option.apply(op.getOutputSchema(Array()))
        } else if (!inputSchemaMap.contains(op) || inputSchemaMap(op).exists(s => s.isEmpty)) {
          // op does not have input, or any of the op's input's output schema is null
          // then this op's output schema cannot be inferred as well
          Option.empty
        } else {
          // op's input schema is complete, try to infer its output schema
          // if inference failed, print an exception message, but still continue the process
          try {
            Option.apply(op.getOutputSchema(inputSchemaMap(op).map(s => s.get).toArray))
          } catch {
            case e: Throwable =>
              e.printStackTrace()
              Option.empty
          }
        }
      }
      // exception: if op is a source operator, use its output schema as input schema for autocomplete
      if (op.isInstanceOf[SourceOperatorDescriptor]) {
        inputSchemaMap.update(op, mutable.MutableList(outputSchema))
      }

      // update input schema of all outgoing links
      val outLinks = this.workflowInfo.links.filter(link => link.origin.operatorID == op.operatorID)
      outLinks.foreach(link => {
        val dest = workflowInfo.operators.find(o => o.operatorID == link.destination.operatorID).get
        // get the input schema list, should be pre-populated with size equals to num of ports
        val destInputSchemas = inputSchemaMap(dest)
        // put the schema into the ordinal corresponding to the port
        destInputSchemas(link.destination.portOrdinal) = outputSchema
        inputSchemaMap.update(dest, destInputSchemas)
      })
    })

    inputSchemaMap
      .filter(e => !(e._2.exists(s => s.isEmpty) || e._2.isEmpty))
      .map(e => (e._1, e._2.toList))
      .toMap
  }

}
