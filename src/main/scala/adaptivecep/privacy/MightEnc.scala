package adaptivecep.privacy

import adaptivecep.privacy.Privacy.{PrivacyContext, PrivacyContextCentralized}
import crypto._
import crypto.dsl._
import crypto.cipher._
import crypto.dsl.Implicits._



sealed trait MightEnc






//
//trait MightEncInt {
//  var value: Either[Int, EncInt]
//
//  var isEncrypted: Boolean
//
//  def change()(implicit privacyContext: PrivacyContext): MightEncInt
//
//  def isEven()(implicit privacyContext: PrivacyContext): Boolean
//
//  def <(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean
//
//  def <=(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean
//
//  def >(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean
//
//  def >=(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean
//
//  def ==(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean
//
//  def !=(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean
//}
//
//case class IntWrapper(v: Int) extends MightEncInt {
//  val value = Left(v)
//
//  override val isEncrypted: Boolean = false
//
//  override def change() (implicit privacyContext: PrivacyContext): MightEncInt = {
//
//     val enc = privacyContext.asInstanceOf[PrivacyContextCentralized].cryptoService.encryptInt(Comparable,v)
//     EncIntWrapper(enc)
//  }
//
//  override def isEven()(implicit privacyContext: PrivacyContext): Boolean =
//    v % 2 == 0
//
//  override def <(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean =
//    value.left.get < other.value.left.get
//
//  override def <=(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean =
//    value.left.get <= other.value.left.get
//
//  override def >(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean =
//    value.left.get > other.value.left.get
//
//
//  override def >=(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean =
//    value.left.get >= other.value.left.get
//
//  override def ==(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean =
//    value.left.get == other.value.left.get
//
//  override def !=(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean =
//    value.left.get != other.value.left.get
//
//}
//
//case class EncIntWrapper(v: EncInt) extends MightEncInt {
//  val value = Right(v)
//
//  override def isEven()(implicit privacyContext: PrivacyContext): Boolean =
//    privacyContext.asInstanceOf[PrivacyContextCentralized].interpret( crypto.dsl.isEven(value.right.get) )
//
//  override def <(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean =
//    privacyContext.asInstanceOf[PrivacyContextCentralized].interpret( value.right.get < other.value.right.get )
//
//  override def <=(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean =
//    privacyContext.asInstanceOf[PrivacyContextCentralized].interpret( value.right.get <= other.value.right.get )
//
//
//  override def >(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean =
//    privacyContext.asInstanceOf[PrivacyContextCentralized].interpret( value.right.get > other.value.right.get )
//
//
//  override def >=(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean =
//    privacyContext.asInstanceOf[PrivacyContextCentralized].interpret( value.right.get >= other.value.right.get )
//
//
//  override def ==(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean =
//    privacyContext.asInstanceOf[PrivacyContextCentralized].interpret( value.right.get == other.value.right.get )
//
//  override def !=(other: MightEncInt)(implicit privacyContext: PrivacyContext): Boolean =
//    privacyContext.asInstanceOf[PrivacyContextCentralized].interpret( value.right.get != other.value.right.get )
//
//  override var isEncrypted: Boolean = true
//
//  override def change()(implicit privacyContext: PrivacyContext): MightEncInt =
//    this
//}
//
