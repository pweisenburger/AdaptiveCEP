package adaptivecep.hotreplacement

import adaptivecep.data.Events.{Init, Update}
import akka.actor.{Actor, ActorRef, Props}
import Integrator._
import adaptivecep.simulation.{UpdateGeneratorLocal, UpdateGeneratorRemote}

object Integrator{
  private var actorsToClass: Map[ActorRef, Class[_]] = Map[ActorRef, Class[_]]()

  def addActor(actor: ActorRef, clazz: Class[_])={
    actorsToClass += (actor -> clazz)
  }

  def deleteActor(actor: ActorRef) = {
    actorsToClass -= actor
  }
}

class Integrator extends Actor{
  val watcher: ActorRef = context.system.actorOf(Props(Watcher))
  val listener: ActorRef = context.system.actorOf(Props(new Listener))
  //val generatorLocal: ActorRef = context.system.actorOf(Props(new UpdateGeneratorLocal))
  //val generatorRemote: ActorRef = context.system.actorOf(Props(new UpdateGeneratorRemote))

  def integrate(clazz: Class[_]) = {
    val iter = actorsToClass.iterator
    while(iter.hasNext){
      val elem = iter.next()
      if(elem._2.getSimpleName.stripSuffix("Proxy") == clazz.getSimpleName.stripSuffix("Impl")){
        elem._1 ! Update(clazz)
      }
    }
  }

  override def receive: Receive = {
    case Init => {
      watcher ! Init
      listener ! Init
      //Thread.sleep(10000)
      //startUpdateSimulation()       //start update simulation
    }
    case Update(clazz: Class[_]) => {
      integrate(clazz)
    }
  }

  def startUpdateSimulation() = {
    //generatorRemote ! Init
    //generatorLocal ! Init
  }

}
