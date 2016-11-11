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

package com.scalarookie.eventscala.backend

import com.scalarookie.eventscala.dsl._

package object esper {

  /********************************************************************************************************************/
  /* `Query` to Esper EPL string                                                                                      */
  /********************************************************************************************************************/

  implicit def query2EplString(query: Query): String = {
    def pattern2EplString(pattern: Pattern): String = pattern match {
      case Stream(clazzName, optionFilter) =>
        val clazzString = clazzName + "=" + clazzName
        if (optionFilter.isEmpty) {
          clazzString
        } else {
          val filter = optionFilter.get
          val operatorString = filter.operator match {
            case Equals => "="
            case Greater => ">"
            case Smaller => "<"
          }
          clazzString + "(" + filter.leftOperand.fieldName + " " + operatorString + " " + filter.rightOperand + ")"
        }
      case At(h, m, s) =>
        val hString = h getOrElse "*"
        val mString = m getOrElse "*"
        val sString = s getOrElse "*"
        "timer:at(" + mString + ", " + hString + ", *, *, *, " + sString + ", *)"
      case After(s) =>
        "timer:interval(" + s + " sec)"
      case Sequence(p1, p2) =>
        val p1String = pattern2EplString(p1)
        val p2String = pattern2EplString(p2)
        "(" + p1String + " -> " + p2String + ")"
      case And(p1, p2) =>
        val p1String = pattern2EplString(p1)
        val p2String = pattern2EplString(p2)
        "(" + p1String + " and " + p2String + ")"
      case Or(p1, p2) =>
        val p1String = pattern2EplString(p1)
        val p2String = pattern2EplString(p2)
        "(" + p1String + " or " + p2String + ")"
      case Not(p) =>
        val pString = pattern2EplString(p)
        "(not " + pString + ")"
    }
    query match {
      case Select(fields, pattern) =>
        val patternString = pattern2EplString(pattern)
        val fieldsString =
          if (fields.isEmpty) "*"
          else fields.map(field => field.clazzName + "." + field.fieldName).mkString(", ")
        "select " + fieldsString + " from pattern [every " + patternString + "]"
    }
  }

}
