package adaptivecep.privacy.phe

import akka.util.Timeout
import crypto.PubKeys
import crypto.dsl.{PureCryptoInterpreter, _}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/***
  * This hides the synchronous call to the actor hosting the actual Interpreter
  * @param actor this actor is initialized as CryptorServiceActor
  *
  */

class CEPRemoteInterpreter(cryptoServiceWrapper: CryptoServiceWrapper) extends PureCryptoInterpreter with Serializable {


  implicit val timeout = Timeout(5 seconds)

  val publicKeys: PubKeys = Await.result(cryptoServiceWrapper.publicKeys,timeout.duration).asInstanceOf[PubKeys]



//
//  override def interpret[A](p: CryptoM[A]): A = {
//    val future = actor ? InterpretRequest(p)
//    val result = Await.result(future,timeout.duration).asInstanceOf[A]
//    resultc
//  }

  /**
    * Interpret a program written in the monadic DSL and return the result
    */
  override def interpret[A](p: _root_.crypto.dsl.CryptoM[A]): A = {
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
    val remoteInterpreter = RemoteInterpreter(cryptoServiceWrapper,publicKeys)
    val result = Await.result(remoteInterpreter.interpret(p),timeout.duration)
    result
  }

}
