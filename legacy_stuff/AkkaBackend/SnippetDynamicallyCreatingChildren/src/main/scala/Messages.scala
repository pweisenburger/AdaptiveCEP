import akka.actor.ActorRef

object Messages {

  case class Created(childNodeId: Int, childNodeRef: ActorRef)
  case class Greet(childNodeId: Int)
  case object Hi

}
