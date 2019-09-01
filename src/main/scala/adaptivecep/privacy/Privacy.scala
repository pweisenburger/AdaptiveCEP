package adaptivecep.privacy

import adaptivecep.privacy.ConversionRules._
import adaptivecep.distributed.operator.TrustedHost
//import adaptivecep.privacy.sgx.EventProcessorServer

//  sealed trait DataSensitivity
//
//  object High extends DataSensitivity
//
//  object Medium extends DataSensitivity
//
//  object Low extends DataSensitivity

object Privacy {

  /***
    * a privacy context contains enough information to distribute privacy preserving methodology to
    * the nodes in the deployed query graph
    */
  sealed trait PrivacyContext extends Serializable {
    def clone: PrivacyContext
  }

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
//  final case class PrivacyContextCentralized( interpret: CEPRemoteInterpreter,
//                                        cryptoService: CryptoServiceWrapper,
//                                        trustedHosts: Set[TrustedHost],
//                                        sourcesSensitivity: Map[String, DataSensitivity]
//                                      ) extends PrivacyContext

  case class SgxPrivacyContext(address: String, port: Int,
                                     conversionRules: Map[String,EventConversionRule]) extends PrivacyContext {
    override def clone: PrivacyContext =
      SgxPrivacyContext(address,port,conversionRules)

  }
//
//  final case class SgxDecentralizedContext(trustedHosts: Set[TrustedHost],
//                                           publisherConversionRules: Map[String,EventConversionRule]
//                                          ) extends PrivacyContext

//  //TODO:
//  /***
//    * this context should combine sgx and phe approaches
//    * @param trustedNodeHost
//    */
//  final case class MixedPrivacyContext(trustedNodeHost: Set[TrustedHost]) extends PrivacyContext

  /**
    * this object will be used for old queries to ensure backwards compatibility
    *
    */
  case object NoPrivacyContext extends PrivacyContext {
    override def clone: PrivacyContext = NoPrivacyContext
  }


}


