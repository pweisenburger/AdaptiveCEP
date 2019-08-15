package adaptivecep.privacy

import adaptivecep.data.Events.Event
import adaptivecep.distributed.operator.TrustedNodeHost
import adaptivecep.privacy.encryption.Encryption
import adaptivecep.privacy.sgx.{EventProcessorClient, EventProcessorServer}
import akka.actor.ActorRef

object Privacy {



  sealed trait DataSensitivity

  object High extends DataSensitivity

  object Medium extends DataSensitivity

  object Low extends DataSensitivity


  sealed trait PrivacyContext

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
                                      ) extends PrivacyContext

  case class SgxPrivacyContext(trustedHosts: Set[TrustedNodeHost],
                               remoteObject: EventProcessorServer,
                               conversionRules: Map[String,EventConversionRule]) extends PrivacyContext

  /***
    * this context should combine sgx and phe approaches
    * @param trustedNodeHost
    */
  case class MixedPrivacyContext(trustedNodeHost: Set[TrustedNodeHost]) extends PrivacyContext

  /**
    * this object will be used for old queries to ensure backwards compatibility
    *
    */
  object NoPrivacyContext extends PrivacyContext


  sealed trait Transformer
  case class EncDecTransformer(encrypt: (Any,Encryption)=> Any,
                               decrypt: (Any,Encryption) => Any) extends Transformer
  object NoTransformer extends Transformer


  sealed trait EventConversionRule extends Serializable

  case class Event1Rule(tr1: Transformer) extends EventConversionRule

  case class Event2Rule(tr1: Transformer, tr2: Transformer) extends EventConversionRule

  case class Event3Rule(tr1: Transformer, tr2: Transformer,tr3: Transformer) extends EventConversionRule

  case class Event4Rule(tr1: Transformer, tr2: Transformer,tr3: Transformer,tr4: Transformer) extends EventConversionRule

  case class Event5Rule(tr1: Transformer, tr2: Transformer,tr3: Transformer,tr4: Transformer, tr5: Transformer) extends EventConversionRule

  case class Event6Rule(tr1: Transformer, tr2: Transformer,tr3: Transformer,tr4: Transformer,tr5: Transformer,tr6: Transformer) extends EventConversionRule






}



