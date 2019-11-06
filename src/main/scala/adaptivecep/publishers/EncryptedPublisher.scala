package adaptivecep.publishers

import java.util.concurrent.TimeUnit

import adaptivecep.data.Events.Event
import scala.concurrent.ExecutionContext.Implicits.global
import adaptivecep.privacy.CryptoServiceWrapper
import akka.actor.ActorRef
import crypto._
import crypto.dsl._
import crypto.cipher._

import scala.concurrent.duration.FiniteDuration

case class EncryptedPublisher(cryptoActor: ActorRef,createEventFromEncId: EncInt => Event)  extends Publisher  {

//  val cryptoSvc = new CryptoServiceWrapper(cryptoActor)
//
//  val publisherName: String = self.path.name
//
//  var id = 0
//  def publish(id: Int): Unit = {
//    val event: Event = createEventFromEncId(cryptoSvc.encryptInt(Comparable, id))    // createEventFromId(id)
//    source._1.offer(event)
//  }
//
//  context.system.scheduler.schedule(
//    initialDelay = FiniteDuration(0, TimeUnit.MILLISECONDS),
//    interval = FiniteDuration(1, TimeUnit.SECONDS),
//    runnable = () => {
//      (1 to 5000).foreach(n => publish(n))
//    })
}
