import Messages.Greet
import akka.actor.ActorSystem

object Main extends App {

  implicit val system = ActorSystem()

  val rootNode = system.actorOf(RootNode.props(5))

  rootNode ! Greet(0)
  rootNode ! Greet(1)
  rootNode ! Greet(2)
  rootNode ! Greet(3)
  rootNode ! Greet(4)
  rootNode ! Greet(5)

  val lucas =5
  val behindert = 3
  if (lucas > behindert)
    println("DÖÖÖÖÖÖÖ!!!")

}



