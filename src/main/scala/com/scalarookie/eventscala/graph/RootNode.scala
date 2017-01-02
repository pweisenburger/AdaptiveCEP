package com.scalarookie.eventscala.graph

import akka.actor.ActorRef
import com.scalarookie.eventscala.caseclasses._

class RootNode(query: Query, publishers: Map[String, ActorRef], callback: Event => Any) extends Node(publishers) {

  val childNode: ActorRef = createChildNode(query, 1)

  override def receive: Receive = {
    case event: Event if sender == childNode =>
      callback(event)
  }

}
