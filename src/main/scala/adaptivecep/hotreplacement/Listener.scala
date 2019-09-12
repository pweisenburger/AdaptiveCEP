package adaptivecep.hotreplacement

import java.net.InetSocketAddress

import adaptivecep.Loader
import adaptivecep.data.Events.{Init, Update}
import akka.actor.{Actor, ActorRef}
import akka.io.{IO, Udp}


class Listener() extends Actor {
  var integrator: ActorRef = null
  var className: String = null
  import context.system

  IO(Udp) ! Udp.Bind(self, new InetSocketAddress("localhost", 7777))

  def receive = {
    case Init =>
      integrator = sender()
    case Udp.Bound(_) =>
      context.become(ready(sender()))
    case _ =>
  }

  def ready(socket: ActorRef): Receive = {
    case Udp.Received(data, _) =>
      var input: Array[Byte] = data.toArray
      if(className == null) {
        className = new String(input)
      } else {
        val clazz = Loader.updateRemote(input, className)
        integrator ! Update(clazz)
        className = null
        socket ! Udp.Unbind
      }
    case Udp.Unbind  =>
      socket ! Udp.Unbind
    case Udp.Unbound => context.become(receive)
  }
}