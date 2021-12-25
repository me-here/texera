package edu.uci.ics.texera.web

import com.google.protobuf.GeneratedMessage
import rx.lang.scala.Subscriber
import scalapb.GeneratedMessage

import scala.collection.mutable

trait SyncableState[T <: GeneratedMessage, U] {

  private var state:T = new T()
  private var isModifying = false

  def modifyState(func:T => T):Unit = {
    synchronized{
      assert(!isModifying, "Cannot recursively modify state or modify state inside computeDiff")
      isModifying = true
      val newState = func(state)
      computeDiff(state, newState).foreach(delta => subscriptionManager.pushToSubscribers(delta))
      isModifying = false
      state = newState
    }
  }

  def subscriptionManager:SubscriptionManager[U]

  def computeDiff(oldState:T, newState:T):Array[U]

  def computeSnapshot:Array[U] = {
    computeDiff(new T(), state)
  }

}


class SubscriptionManager[U]{
  private val subscribers:mutable.ArrayBuffer[Subscriber[U]] = mutable.ArrayBuffer.empty

  def addSubscriber(subscriber: Subscriber[U]):Unit = {
    subscribers += subscriber
  }

  def removeSubscriber(subscriber: Subscriber[U]):Unit = {
    subscribers -= subscriber
  }

  def pushToSubscribers(msg:U):Unit = {
    subscribers.foreach(subscriber => subscriber.onNext(msg))
  }
}
