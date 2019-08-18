package adaptivecep.privacy

import adaptivecep.data.Events._
import adaptivecep.privacy.encryption.Encryption

object ConversionRules {

  sealed trait Transformer extends Serializable

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


  private def applyTransformer(data: Any, transformer: Transformer)(implicit encryption: Encryption): Any = {
    transformer match {
      case NoTransformer => data
      case EncDecTransformer(encrypt, decrypt) => encrypt(data, encryption)
    }
  }

  def getEncryptedEvent(e: Event, conversionRule: EventConversionRule)(implicit encryption: Encryption): EncEvent = {
    (e, conversionRule) match {
      case (Event1(e1), er: Event1Rule) => EncEvent1(applyTransformer(e1, er.tr1), er)
      case (Event2(e1, e2), er2: Event2Rule) =>
        EncEvent2(
          applyTransformer(e1, er2.tr1),
          applyTransformer(e2, er2.tr2),
          er2)
      case (Event3(e1, e2, e3), er3: Event3Rule) =>
        EncEvent3(
          applyTransformer(e1, er3.tr1),
          applyTransformer(e2, er3.tr2),
          applyTransformer(e3, er3.tr3),
          er3
        )
      case (Event4(e1, e2, e3, e4), er4: Event4Rule) =>
        EncEvent4(
          applyTransformer(e1, er4.tr1),
          applyTransformer(e2, er4.tr2),
          applyTransformer(e3, er4.tr3),
          applyTransformer(e4, er4.tr4),
          er4
        )
      case (Event5(e1, e2, e3, e4, e5), er5: Event5Rule) =>
        EncEvent5(
          applyTransformer(e1, er5.tr1),
          applyTransformer(e2, er5.tr2),
          applyTransformer(e3, er5.tr3),
          applyTransformer(e4, er5.tr4),
          applyTransformer(e5, er5.tr5),
          er5
        )
      case (Event6(e1, e2, e3, e4, e5, e6), er6: Event6Rule) =>
        EncEvent6(
          applyTransformer(e1, er6.tr1),
          applyTransformer(e2, er6.tr2),
          applyTransformer(e3, er6.tr3),
          applyTransformer(e4, er6.tr4),
          applyTransformer(e5, er6.tr5),
          applyTransformer(e6, er6.tr6),
          er6
        )
    }
  }

  def getDecryptedEvent(e: EncEvent)(implicit encryption: Encryption): Event = {
    e match {
      case EncEvent1(e1, rule) =>
        Event1(applyReverseTransformer(e1, rule.tr1))
      case EncEvent2(e1, e2, rule) =>
        Event2(
          applyReverseTransformer(e1, rule.tr1),
          applyReverseTransformer(e2, rule.tr2)
        )
      case EncEvent3(e1, e2, e3, rule) =>
        Event3(
          applyReverseTransformer(e1, rule.tr1),
          applyReverseTransformer(e2, rule.tr2),
          applyReverseTransformer(e3, rule.tr3)
        )

      case EncEvent4(e1, e2, e3, e4, rule) =>
        Event4(
          applyReverseTransformer(e1, rule.tr1),
          applyReverseTransformer(e2, rule.tr2),
          applyReverseTransformer(e3, rule.tr3),
          applyReverseTransformer(e4, rule.tr4)
        )

      case EncEvent5(e1, e2, e3, e4, e5, rule) =>
        Event5(
          applyReverseTransformer(e1, rule.tr1),
          applyReverseTransformer(e2, rule.tr2),
          applyReverseTransformer(e3, rule.tr3),
          applyReverseTransformer(e4, rule.tr4),
          applyReverseTransformer(e5, rule.tr5)
        )

      case EncEvent6(e1, e2, e3, e4, e5, e6, rule) =>
        Event6(
          applyReverseTransformer(e1, rule.tr1),
          applyReverseTransformer(e2, rule.tr2),
          applyReverseTransformer(e3, rule.tr3),
          applyReverseTransformer(e4, rule.tr4),
          applyReverseTransformer(e5, rule.tr5),
          applyReverseTransformer(e6, rule.tr6)
        )

    }


  }

  private def applyReverseTransformer(data: Any, transformer: Transformer)(implicit encryption: Encryption): Any = {
    transformer match {
      case NoTransformer => data
      case EncDecTransformer(encrypt, decrypt) => decrypt(data, encryption)
    }
  }


}
