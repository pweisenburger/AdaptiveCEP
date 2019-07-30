package adaptivecep.privacy
import akka.pattern.ask
import akka.actor.ActorRef
import akka.util.Timeout

import scala.concurrent.duration._
import crypto.dsl._
import adaptivecep.data.Events._
import crypto.dsl.{CryptoInterpreter, PureCryptoInterpreter}

import scala.concurrent.{Await, Future}

case class CEPRemoteInterpreter(actor: ActorRef) extends PureCryptoInterpreter{
  /**
    * Interpret a program written in the monadic DSL and return the result
    */
  /**
    * Interpret a program written in the monadic DSL and return the result
    */
  implicit val timeout = Timeout(5 seconds)

  override def interpret[A](p: CryptoM[A]): A = {
    val future = actor ? InterpretRequest(p)
    val result = Await.result(future,timeout.duration).asInstanceOf[A]
    result
  }

}
