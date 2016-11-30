package com.scalarookie.eventscala.graph

import akka.actor.{Actor, ActorRef}
import com.scalarookie.eventscala.caseclasses._

class RootNode(query: Query, publishers: Map[String, ActorRef], callback: Event => Any) extends Actor {

  val nodeName: String = self.path.name

  val childNode: ActorRef =
    Node.createChildNodeFrom(query, nodeName, 1, publishers, context)

  override def receive: Receive = {
    case event: Event if sender == childNode =>
      callback(event)
  }

}
