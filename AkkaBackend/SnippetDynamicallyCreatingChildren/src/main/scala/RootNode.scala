import Messages.{Created, Greet, Hi}
import akka.actor.{Actor, ActorRef, Props}

object RootNode {

  def props(nrOfChildren: Int) =
    Props(new RootNode(nrOfChildren))

}

class RootNode(nrOfChildren: Int) extends Actor {

  println("RootNode has been created.")

  if (nrOfChildren > 0)
    context.actorOf(ChildNode.props(nrOfChildren))

  var childNodeRefs: Map[Int, ActorRef] = Map.empty

  var greetingsQueue: List[Greet] = List.empty

  def receive = {
    case Created(childNodeId, childNodeRef) =>
      println("Newly created ChildNode " + childNodeId + " has reported back to RootNode.")
      childNodeRefs = childNodeRefs + (childNodeId -> childNodeRef)
      if (childNodeRefs.size == nrOfChildren + 1) {
        println("All ChildNode actors have reported back to RootNode.")
        greetingsQueue.foreach(greeting => {
          childNodeRefs(greeting.childNodeId) ! Hi
          println("A queued greeting has been sent to ChildNode " + greeting.childNodeId + ".")
        })
      }
    case Greet(childNodeId) =>
      if (childNodeRefs.size != nrOfChildren) {
        greetingsQueue = greetingsQueue ::: List(Greet(childNodeId))
        println("Greeting to ChildNode " + childNodeId + " has been queued.")
      } else {
        childNodeRefs(childNodeId) ! Hi
        println("A greeting has been sent to ChildNode " + childNodeId + ".")
      }
  }

}
