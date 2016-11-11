package frontend

import middleend._

object Dsl {

  implicit def string2PrimitiveQuery(string: String): PrimitiveQuery =
    PrimitiveQuery(string)



}
