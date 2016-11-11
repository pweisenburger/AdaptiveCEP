import Messages.{Created, Hi}
import akka.actor.{Actor, ActorRef, Props}

object ChildNode {
  def props(nrOfChildren: Int) = Props(new ChildNode(nrOfChildren))
}

class ChildNode(nrOfChildren: Int) extends Actor {

  context.parent ! Created(nrOfChildren, self)

  if (nrOfChildren > 0)
    context.actorOf(ChildNode.props(nrOfChildren - 1))

  def receive = {
    case Created(childNodeId, childNodeRef) =>
      context.parent ! Created(childNodeId, childNodeRef)
    case Hi =>
      println("ChildNode " + nrOfChildren + " has been said hi to.")
  }

}
