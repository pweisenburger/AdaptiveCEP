package adaptivecep.privacy

import adaptivecep.distributed.operator.TrustedNodeHost

object Privacy {

  sealed trait PrivacyContext

  sealed trait DataSensitivity

  object High extends DataSensitivity
  object Medium extends DataSensitivity
  object Low extends DataSensitivity

  case class PrivacyContextCentralized ( remoteInterpreter: CEPRemoteInterpreter,
                                         cryptoServiceWrapper: CryptoServiceWrapper,
                                         trustedHosts: Set[TrustedNodeHost],
                                         sourcesSensitivity: Map[String,DataSensitivity]
                                       ) extends PrivacyContext

   object NoPrivacyContext extends PrivacyContext



}

