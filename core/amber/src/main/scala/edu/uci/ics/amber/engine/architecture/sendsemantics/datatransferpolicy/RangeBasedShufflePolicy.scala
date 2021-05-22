package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Notes for later:
  *
  * I will keep a heavy hitter list (top k). When asked to share load, I will start checking the incoming tuple to see if it
  * is a heavy hitter. If yes, then I will send it to the free or skewed in round robin manner. But if not heavy hitter, it will
  * always go to the skewed.
  *
  * @param policyTag
  * @param batchSize
  * @param hashFunc
  * @param receivers
  */

class RangeBasedShufflePolicy(
    policyTag: LinkIdentity,
    batchSize: Int,
    val rangeFunc: ITuple => Int,
    val shuffleKey: ITuple => String,
    receivers: Array[ActorVirtualIdentity]
) extends ParallelBatchingPolicy(policyTag, batchSize, receivers) {
  val numBuckets = receivers.length

  // buckets once decided will remain same because we are not changing the number of workers in Join
  var bucketsToReceivers = new mutable.HashMap[Int, ArrayBuffer[ActorVirtualIdentity]]()
  var nextReceiverIdxInBucket = new mutable.HashMap[Int, Int]()
  var receiverToBatch = new mutable.HashMap[ActorVirtualIdentity, Array[ITuple]]()
  var receiverToCurrBatchSize = new mutable.HashMap[ActorVirtualIdentity, Int]()
  var receiverToTotalSent = new mutable.HashMap[ActorVirtualIdentity, Long]()

  initializeInternalState(receivers)

  override def noMore(): Array[(ActorVirtualIdentity, DataPayload)] = {
    println(
      s"No more received for worker. Already sent out ${receiverToTotalSent.mkString("\n\t")}"
    )
    val receiversAndBatches = new ArrayBuffer[(ActorVirtualIdentity, DataPayload)]
    for ((receiver, currSize) <- receiverToCurrBatchSize) {
      if (currSize > 0) {
        receiversAndBatches.append(
          (receiver, DataFrame(receiverToBatch(receiver).slice(0, currSize)))
        )
      }
      receiversAndBatches.append((receiver, EndOfUpstream()))
    }
    receiversAndBatches.toArray
  }

  // for non-heavy hitters get the default receiver
  private def getDefaultReceiverForBucket(bucket: Int): ActorVirtualIdentity =
    bucketsToReceivers(bucket)(0)

  // to be called for heavy-hitter
  private def getAndIncrementReceiverForBucket(bucket: Int): ActorVirtualIdentity = {
    var receiver: ActorVirtualIdentity = null
    var nextReceiverIdx = -1
    if (nextReceiverIdxInBucket(bucket) >= bucketsToReceivers(bucket).size) {
      // may happen if `removeReceiverFromBucket()` removes the receiver pointed by the `nextReceiverIdxInBucket(bucket)`
      receiver = bucketsToReceivers(bucket)(0)
      nextReceiverIdx = 1
    } else {
      receiver = bucketsToReceivers(bucket)(nextReceiverIdxInBucket(bucket))
      nextReceiverIdx = nextReceiverIdxInBucket(bucket) + 1
    }

    if (nextReceiverIdx >= bucketsToReceivers(bucket).size) {
      nextReceiverIdx = 0
    }
    nextReceiverIdxInBucket(bucket) = nextReceiverIdx
    receiver
  }

  override def addReceiverToBucket(
      defaultRecId: ActorVirtualIdentity,
      newRecId: ActorVirtualIdentity
  ): Map[ActorVirtualIdentity, Long] = {
    var defaultBucket: Int = -1
    bucketsToReceivers.keys.foreach(b => {
      if (bucketsToReceivers(b)(0) == defaultRecId) { defaultBucket = b }
    })
    assert(defaultBucket != -1)
    println(
      s"\tAdd receiver to bucket received. Already sent out ${defaultRecId}:${receiverToTotalSent.getOrElse(
        defaultRecId,
        0
      )};;;${newRecId}:${receiverToTotalSent.getOrElse(newRecId, 0)}"
    )
    if (!bucketsToReceivers(defaultBucket).contains(newRecId)) {
      for (i <- 1 to Constants.skewToFreeTransferRatio - 1) {
        // adding defaultRecId multiple times in bucket to simulate Constants.skewToFreeTransferRatio:1
        // data transfer ratio between skewed and free worker
        bucketsToReceivers(defaultBucket).append(defaultRecId)
      }
      bucketsToReceivers(defaultBucket).append(newRecId)
    }
    receiverToTotalSent.toMap
  }

  override def removeReceiverFromBucket(
      defaultRecId: ActorVirtualIdentity,
      recIdToRemove: ActorVirtualIdentity
  ): Map[ActorVirtualIdentity, Long] = {
    var defaultBucket: Int = -1
    bucketsToReceivers.keys.foreach(b => {
      if (bucketsToReceivers(b)(0) == defaultRecId) { defaultBucket = b }
    })
    assert(defaultBucket != -1)
    var idxToRemove = -1
    for (i <- 0 to bucketsToReceivers(defaultBucket).size - 1) {
      if (bucketsToReceivers(defaultBucket)(i) == recIdToRemove) { idxToRemove = i }
    }
    assert(idxToRemove != -1)
    println(
      s"\tRemove receiver from bucket received. Already sent out ${defaultRecId}:${receiverToTotalSent.getOrElse(
        defaultRecId,
        0
      )};;;${recIdToRemove}:${receiverToTotalSent.getOrElse(recIdToRemove, 0)}"
    )
    bucketsToReceivers(defaultBucket).remove(idxToRemove)
    if (idxToRemove > 1) {
      // this is a hack. Currently, we have only one free worker per skewed worker. So, we add only one free worker in a
      // bucket. To ensure that the load gets shared Constants.skewToFreeTransferRatio:1 between skewed and free worker
      // we put the default worker Constants.skewToFreeTransferRatio times in the bucket, in @addReceiverToBucket .
      // Now we should remove it.
      for (i <- 1 to Constants.skewToFreeTransferRatio - 1) {
        if (bucketsToReceivers(defaultBucket).size >= 2) {
          bucketsToReceivers(defaultBucket).remove(bucketsToReceivers(defaultBucket).size - 1)
        }
      }
    }
    receiverToTotalSent.toMap
  }

  private def isHeavyHitterTuple(key: String) = {
    true
  }

  override def addTupleToBatch(
      tuple: ITuple
  ): Option[(ActorVirtualIdentity, DataPayload)] = {
    val index = selectBatchingIndex(tuple)
    var receiver: ActorVirtualIdentity = null
    if (bucketsToReceivers(index).size > 1 && isHeavyHitterTuple(shuffleKey(tuple))) {
      // choose one of the receivers in round robin manner
      // println("GOING ROUND ROBIN")
      receiver = getAndIncrementReceiverForBucket(index)
    } else {
      receiver = getDefaultReceiverForBucket(index)
    }
    receiverToBatch(receiver)(receiverToCurrBatchSize(receiver)) = tuple
    receiverToTotalSent(receiver) = receiverToTotalSent.getOrElse(receiver, 0L) + 1
    receiverToCurrBatchSize(receiver) += 1
    if (receiverToCurrBatchSize(receiver) == batchSize) {
      receiverToCurrBatchSize(receiver) = 0
      val retBatch = receiverToBatch(receiver)
      receiverToBatch(receiver) = new Array[ITuple](batchSize)
      return Some((receiver, DataFrame(retBatch)))
    }
    None
  }

  override def reset(): Unit = {
    initializeInternalState(receivers)
  }

  private[this] def initializeInternalState(_receivers: Array[ActorVirtualIdentity]): Unit = {
    for (i <- 0 until numBuckets) {
      bucketsToReceivers(i) = ArrayBuffer[ActorVirtualIdentity](receivers(i))
      nextReceiverIdxInBucket(i) = 0
      receiverToBatch(_receivers(i)) = new Array[ITuple](batchSize)
      receiverToCurrBatchSize(_receivers(i)) = 0
    }
  }

  override def selectBatchingIndex(tuple: ITuple): Int = {
    val numBuckets = receivers.length
    rangeFunc(tuple)
  }
}
