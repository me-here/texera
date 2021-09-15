package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.DataSendingPolicy
import edu.uci.ics.amber.engine.common.ambermessage.DataPayload
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

/** This class is a container of all the transfer policies.
  * @param selfID
  * @param dataOutputPort
  * @param controlOutputPort
  */
class TupleToBatchConverter(
    selfID: ActorVirtualIdentity,
    dataOutputPort: DataOutputPort
) {
  private var policies = new Array[DataSendingPolicy](0)

  /** Add down stream operator and its transfer policy
    * @param policy
    * @param linkTag
    * @param receivers
    */
  def addPolicy(
      policy: DataSendingPolicy
  ): Unit = {
    var i = 0
    Breaks.breakable {
      while (i < policies.length) {
        if (policies(i).policyTag == policy.policyTag) {
          policies(i) = policy
          Breaks.break()
        }
        i += 1
      }
      policies = policies :+ policy
    }
  }

  def getWorkloadHistory(): mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]] = {
    var ret: mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]] = null
    policies.foreach(policy => {
      ret = policy.getWorkloadHistory(selfID)
    })
    if (ret == null) {
      ret = new mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]
    }
    ret
  }

  def recordHistory(): Unit = {
    policies.foreach(policy => {
      policy.recordHistory = true
    })
  }

  def changeFlow(
      skewedReceiverId: ActorVirtualIdentity,
      freeReceiverId: ActorVirtualIdentity,
      tuplesToRedirectNumerator: Long,
      tuplesToRedirectDenominator: Long
  ): Map[ActorVirtualIdentity, Long] = {
    var receiverToSentCount: Map[ActorVirtualIdentity, Long] = null
    policies.foreach(policy => {
      receiverToSentCount = policy.addReceiverToBucket(
        skewedReceiverId,
        freeReceiverId,
        tuplesToRedirectNumerator,
        tuplesToRedirectDenominator
      )
    })
    if (receiverToSentCount == null) {
      receiverToSentCount = new mutable.HashMap[ActorVirtualIdentity, Long]().toMap
    }
    receiverToSentCount
  }

  def rollbackFlow(
      skewedReceiverId: ActorVirtualIdentity,
      freeReceiverId: ActorVirtualIdentity
  ): Map[ActorVirtualIdentity, Long] = {
    var receiverToSentCount: Map[ActorVirtualIdentity, Long] = null
    policies.foreach(policy => {
      receiverToSentCount = policy.removeReceiverFromBucket(skewedReceiverId, freeReceiverId)
    })
    if (receiverToSentCount == null) {
      receiverToSentCount = new mutable.HashMap[ActorVirtualIdentity, Long]().toMap
    }
    receiverToSentCount
  }

  /** Push one tuple to the downstream, will be batched by each transfer policy.
    * Should ONLY be called by DataProcessor.
    * @param tuple
    */
  def passTupleToDownstream(tuple: ITuple): Unit = {
    var i = 0
    while (i < policies.length) {
      val receiverAndBatch: Option[(ActorVirtualIdentity, DataPayload)] =
        policies(i).addTupleToBatch(tuple)
      receiverAndBatch match {
        case Some((id, batch)) =>
          // send it to messaging layer to be sent downstream
          dataOutputPort.sendTo(id, batch)
        case None =>
        // Do nothing
      }
      i += 1
    }
  }

  /* Old API: for compatibility */
  def resetPolicies(): Unit = {
    policies.foreach(_.reset())
  }

  /* Send the last batch and EOU marker to all down streams */
  def emitEndOfUpstream(): Unit = {
    var i = 0
    while (i < policies.length) {
      val receiversAndBatches: Array[(ActorVirtualIdentity, DataPayload)] = policies(i).noMore()
      receiversAndBatches.foreach {
        case (id, batch) => dataOutputPort.sendTo(id, batch)
      }
      i += 1
    }
  }

}
