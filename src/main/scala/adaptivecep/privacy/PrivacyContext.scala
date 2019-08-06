package adaptivecep.privacy

import adaptivecep.distributed.operator.TrustedNodeHost
import akka.actor.ActorRef

object Privacy {

  sealed trait PrivacyContext


  sealed trait DataSensitivity

  object High extends DataSensitivity

  object Medium extends DataSensitivity

  object Low extends DataSensitivity



  /**
    * this class will be used by a type that should encrypt the data as needed
    *
    * @param interpret     the interpreter that will be used to
    *                      interpret expressions from secure scala
    * @param cryptoService a wrapper around the CryptoService Actor
    *                      to hide the dependency on Akka actors
    * @param trustedHosts  Specifies which host actor is trusted
    *                      Assumptions:
    *                      1) consumer host is always trusted
    *                      2) publisher host is always trusted
    *                      3) CryptoService host is always trusted
    * @param sourcesSensitivity specifies the source(publishers) names
    *                      and how sensitive the data coming from there
    *                      this is on Host level, we need to change it to
    *                      property(field) level
    */
  case class PrivacyContextCentralized( interpret: CEPRemoteInterpreter,
                                        cryptoService: CryptoServiceWrapper,
                                        trustedHosts: Set[TrustedNodeHost],
                                        sourcesSensitivity: Map[String, DataSensitivity]
                                      ) extends PrivacyContext {}

  /**
    * this object will be used for old queries to ensure backwards compatibility
    *
    */
  object NoPrivacyContext extends PrivacyContext


}

