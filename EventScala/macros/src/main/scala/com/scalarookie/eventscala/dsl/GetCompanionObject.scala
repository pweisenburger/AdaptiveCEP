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

import scala.annotation.{StaticAnnotation, compileTimeOnly}
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

@compileTimeOnly("@GetCompanionObject needs macro paradise to be enabled in order to expand.")
class GetCompanionObject[T] extends StaticAnnotation {
  def macroTransform(annottees: Any*) =
    macro GetCompanionObject.expandImpl
}

object GetCompanionObject {
  def expandImpl(c: blackbox.Context)(annottees: c.Tree*) = {
    import c.universe._
    val errorMsg = "@GetCompanionObject has not been called correctly. For some class `A`, a correct call would look " +
      "like this: `@GetCompanionObject[A] object A`."
    c.prefix.tree match {
      case q"new GetCompanionObject[$t]" =>
        val clazzType: c.Type = c.typecheck(tq"$t", c.TYPEmode).tpe
        val clazzName: String = clazzType.typeSymbol.name.toString
        val clazzFields: List[c.Symbol] = clazzType.members.filter(member => !member.isMethod).toList
        val fieldNames: List[String] = clazzFields.map(field => field.name.toString.trim)
        val fieldTypes: List[c.Type] = clazzFields.map(field => field.typeSignature)
        annottees match {
          case List(q"object $objectName") =>
            val objectFields: List[c.Tree] = fieldNames.zip(fieldTypes).map(fieldNameType => q"""
              val ${TermName(fieldNameType._1)} =
                Field[$clazzType, ${fieldNameType._2}]($clazzName, ${fieldNameType._1})
              """)
            c.Expr(q"""
              object $objectName {
                ..$objectFields
                def apply() =
                  Stream[$clazzType]($clazzName, None)
                def apply(filter: Filter[$clazzType]) =
                  Stream[$clazzType]($clazzName, Some(filter))
              }
            """)
          case _ => c.abort(c.enclosingPosition, errorMsg)
        }
      case _ => c.abort(c.enclosingPosition, errorMsg)
    }
  }
}
