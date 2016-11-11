/**********************************************************************************************************************/
/*                                                                                                                    */
/* EventScala                                                                                                         */
/*                                                                                                                    */
/* Developed by                                                                                                       */
/* Lucas Bärenfänger (@scalarookie)                                                                                   */
/*                                                                                                                    */
/* Visit scalarookie.com for more information.                                                                        */
/*                                                                                                                    */
/**********************************************************************************************************************/

package com.scalarookie.eventscala

package object dsl {

  /********************************************************************************************************************/
  /* Query - Select                                                                                                   */
  /********************************************************************************************************************/

  def select(fields: Field[_, _]*)(pattern: Pattern): Query = Select(fields.toList, pattern)

  /********************************************************************************************************************/
  /* Pattern - Primitive representing a stream of instances of a class                                                */
  /********************************************************************************************************************/

  class FilterHelper[ClazzType](leftOperand: Field[ClazzType, Int]) {
    def ===(rightOperand: Int): Filter[ClazzType] = Filter[ClazzType](Equals, leftOperand, rightOperand)
    def <(rightOperand: Int): Filter[ClazzType] = Filter[ClazzType](Smaller, leftOperand, rightOperand)
    def >(rightOperand: Int): Filter[ClazzType] = Filter[ClazzType](Greater, leftOperand, rightOperand)
  }

  implicit def field2FilterHelper[ClazzType](field: Field[ClazzType, Int]): FilterHelper[ClazzType] =
    new FilterHelper[ClazzType](field)

  /********************************************************************************************************************/
  /* Pattern - Primitive representing a stream of instances of time - AtEvery                                         */
  /********************************************************************************************************************/

  implicit def int2OptionInt(i: Int): Option[Int] =
    Some(i)

  case object **

  implicit def stars2OptionInt(stars: **.type): Option[Int] =
    None

  type Hour = Option[Int]
  type HourMinute = (Option[Int], Option[Int])
  type HourMinuteSecond = (Option[Int], Option[Int], Option[Int])

  class AtEveryArguments(val hour: Option[Int], val minute: Option[Int], val second: Option[Int])

  class HourHelper(hour: Hour) {
    def h(minute: Option[Int]): HourMinute =
      (hour, minute)
  }

  implicit def int2HourHelper(i: Int): HourHelper =
    new HourHelper(Some(i))

  implicit def stars2HourHelper(stars: **.type): HourHelper =
    new HourHelper(None)

  class HourMinuteHelper(hourMinute: HourMinute) {
    def m(second: Option[Int]): HourMinuteSecond =
      (hourMinute._1, hourMinute._2, second)
  }

  implicit def hourMinute2HourMinuteHelper(hourMinute: HourMinute): HourMinuteHelper =
    new HourMinuteHelper(hourMinute)

  class HourMinuteSecondHelper(hourMinuteSecond: HourMinuteSecond) {
    def s: AtEveryArguments =
      new AtEveryArguments(hourMinuteSecond._1, hourMinuteSecond._2, hourMinuteSecond._3)
  }

  implicit def hourMinuteSecond2HourMinuteSecondHelper(hourMinuteSecond: HourMinuteSecond): HourMinuteSecondHelper =
    new HourMinuteSecondHelper(hourMinuteSecond)

  case class AtEvery(arguments: AtEveryArguments)

  implicit def atEvery2Pattern(atEvery: AtEvery): Pattern =
    At(atEvery.arguments.hour, atEvery.arguments.minute, atEvery.arguments.second)

  implicit def atEvery2PattenHelper(atEvery: AtEvery): PatternHelper =
    At(atEvery.arguments.hour, atEvery.arguments.minute, atEvery.arguments.second)

  /********************************************************************************************************************/
  /* Pattern - Primitive representing a stream of instances of time - AfterEvery                                      */
  /********************************************************************************************************************/

  type Seconds = Int

  class AfterEveryArguments(val seconds: Seconds)

  class SecondsHelper(seconds: Seconds) {
    def s =
      new AfterEveryArguments(seconds)
  }

  implicit def seconds2SecondsHelper(seconds: Seconds): SecondsHelper =
    new SecondsHelper(seconds)

  case class AfterEvery(arguments: AfterEveryArguments)

  implicit def afterEvery2Pattern(afterEvery: AfterEvery): Pattern =
    After(afterEvery.arguments.seconds)

  implicit def afterEvery2PatternHelper(afterEvery: AfterEvery): PatternHelper =
    After(afterEvery.arguments.seconds)

  /********************************************************************************************************************/
  /* Pattern - Operators                                                                                              */
  /********************************************************************************************************************/

  case class PatternHelper(pattern: Pattern) {
    def ->(patternHelper: PatternHelper) =
      Sequence(pattern, patternHelper)
    def &(patternHelper: PatternHelper) =
      And(pattern, patternHelper)
    def |(patternHelper: PatternHelper) =
      Or(pattern, patternHelper)
    def unary_! =
      Not(pattern)
  }

  implicit def patternHelper2Pattern(patternHelper: PatternHelper): Pattern =
    patternHelper.pattern

  implicit def pattern2PatternHelper(pattern: Pattern): PatternHelper =
    PatternHelper(pattern)

}
