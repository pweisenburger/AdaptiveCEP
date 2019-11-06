package adaptivecep.privacy
import akka.pattern.ask
import akka.actor.ActorRef
import akka.util.Timeout

import scala.concurrent.duration._
import crypto.dsl._
import adaptivecep.data.Events._
import crypto.PubKeys
import crypto.dsl.{CryptoInterpreter, PureCryptoInterpreter}

import scala.concurrent.{Await, ExecutionContext, Future}

/***
  * This hides the synchronous call to the actor hosting the actual Interpreter
  * @param actor this actor is initialized as CryptorServiceActor
  *
  */

class CEPRemoteInterpreter(cryptoServiceWrapper: CryptoServiceWrapper) extends PureCryptoInterpreter with Serializable {

  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
    implicit val timeout = Timeout(5 seconds)

  val publicKeys: PubKeys = Await.result(cryptoServiceWrapper.publicKeys,timeout.duration).asInstanceOf[PubKeys]

  val remoteInterpreter = RemoteInterpreter(cryptoServiceWrapper,publicKeys)


//
//  override def interpret[A](p: CryptoM[A]): A = {
//    val future = actor ? InterpretRequest(p)
//    val result = Await.result(future,timeout.duration).asInstanceOf[A]
//    resultc
//  }

  /**
    * Interpret a program written in the monadic DSL and return the result
    */
  override def interpret[A](p: _root_.crypto.dsl.CryptoM[A]): A =
    Await.result(remoteInterpreter.interpret(p),timeout.duration)
}
