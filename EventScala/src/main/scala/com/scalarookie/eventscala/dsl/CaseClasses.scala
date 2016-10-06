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

package com.scalarookie.eventscala.dsl

/**********************************************************************************************************************/
/* Query                                                                                                              */
/**********************************************************************************************************************/

sealed trait Query

/**********************************************************************************************************************/
/* Query - Select                                                                                                     */
/**********************************************************************************************************************/

case class Select(fields: List[Field[_, _]], pattern: Pattern) extends Query
case class Field[ClazzType, FieldType](clazzName: String, fieldName: String)

/**********************************************************************************************************************/
/* Pattern                                                                                                            */
/**********************************************************************************************************************/

sealed trait Pattern

/**********************************************************************************************************************/
/* Pattern - Primitive representing a stream of instances of a class                                                  */
/**********************************************************************************************************************/

case class Stream[ClazzType](clazzName: String, filter: Option[Filter[ClazzType]]) extends Pattern
case class Filter[ClazzType](operator: FilterOperator, leftOperand: Field[ClazzType, Int], rightOperand: Int)

sealed trait FilterOperator
case object Equals extends FilterOperator
case object Smaller extends FilterOperator
case object Greater extends FilterOperator

/**********************************************************************************************************************/
/* Pattern - Primitive representing a stream of instances of time - At                                                */
/**********************************************************************************************************************/

case class At(hour: Option[Int], minute: Option[Int], second: Option[Int]) extends Pattern

/**********************************************************************************************************************/
/* Pattern - Primitive representing a stream of instances of time - After                                             */
/**********************************************************************************************************************/

case class After(secs: Int) extends Pattern

/**********************************************************************************************************************/
/* Pattern - Operators                                                                                                */
/**********************************************************************************************************************/

case class Sequence(p1: Pattern, p2: Pattern) extends Pattern
case class And(p1: Pattern, p2: Pattern) extends Pattern
case class Or(p1: Pattern, p2: Pattern) extends Pattern
case class Not(p: Pattern) extends Pattern
