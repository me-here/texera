package edu.uci.ics.texera.web

import rx.lang.scala.Subscription

import scala.collection.mutable

trait SubscriptionManager {

  private val subscriptions = mutable.ArrayBuffer[Subscription]()

  def addSubscription(sub: Subscription): Unit = {
    subscriptions.append(sub)
  }

  def unsubscribeAll(): Unit = {
    subscriptions.foreach(_.unsubscribe())
    subscriptions.clear()
  }

}
