package com.scalarookie.eventscala.experimental

object Typesafety extends App {

  /********************************************************************************************************************/
  /* Case classes                                                                                                     */
  /********************************************************************************************************************/

  sealed trait Query_1[A]
  sealed trait Query_2[A, B]
  sealed trait Query_3[A, B, C]
  sealed trait Query_4[A, B, C, D]
  sealed trait Query_5[A, B, C, D, E]
  sealed trait Query_6[A, B, C, D, E, F]

  case class Stream_1[A](name: String) extends Query_1[A]
  case class Stream_2[A, B](name: String) extends Query_2[A, B]
  case class Stream_3[A, B, C](name: String) extends Query_3[A, B, C]
  case class Stream_4[A, B, C, D](name: String) extends Query_4[A, B, C, D]
  case class Stream_5[A, B, C, D, E](name: String) extends Query_5[A, B, C, D, E]
  case class Stream_6[A, B, C, D, E, F](name: String) extends Query_6[A, B, C, D, E, F]

  case class KeepEventsWith_1[A](q: Query_1[A]) extends Query_1[A]
  case class KeepEventsWith_2[A, B](q: Query_2[A, B]) extends Query_2[A, B]
  case class KeepEventsWith_3[A, B, C](q: Query_3[A, B, C]) extends Query_3[A, B, C]
  case class KeepEventsWith_4[A, B, C, D](q: Query_4[A, B, C, D]) extends Query_4[A, B, C, D]
  case class KeepEventsWith_5[A, B, C, D, E](q: Query_5[A, B, C, D, E]) extends Query_5[A, B, C, D, E]
  case class KeepEventsWith_6[A, B, C, D, E, F](q: Query_6[A, B, C, D, E, F]) extends Query_6[A, B, C, D, E, F]

  case class RemoveElement1_1[A, B](q: Query_2[A, B]) extends Query_1[B]
  case class RemoveElement2_1[A, B](q: Query_2[A, B]) extends Query_1[A]
  case class RemoveElement1_2[A, B, C](q: Query_3[A, B, C]) extends Query_2[B, C]
  case class RemoveElement2_2[A, B, C](q: Query_3[A, B, C]) extends Query_2[A, C]
  case class RemoveElement3_2[A, B, C](q: Query_3[A, B, C]) extends Query_2[A, B]
  case class RemoveElement1_3[A, B, C, D](q: Query_4[A, B, C, D]) extends Query_3[B, C, D]
  case class RemoveElement2_3[A, B, C, D](q: Query_4[A, B, C, D]) extends Query_3[A, C, D]
  case class RemoveElement3_3[A, B, C, D](q: Query_4[A, B, C, D]) extends Query_3[A, B, D]
  case class RemoveElement4_3[A, B, C, D](q: Query_4[A, B, C, D]) extends Query_3[A, B, C]
  case class RemoveElement1_4[A, B, C, D, E](q: Query_5[A, B, C, D, E]) extends Query_4[B, C, D, E]
  case class RemoveElement2_4[A, B, C, D, E](q: Query_5[A, B, C, D, E]) extends Query_4[A, C, D, E]
  case class RemoveElement3_4[A, B, C, D, E](q: Query_5[A, B, C, D, E]) extends Query_4[A, B, D, E]
  case class RemoveElement4_4[A, B, C, D, E](q: Query_5[A, B, C, D, E]) extends Query_4[A, B, C, E]
  case class RemoveElement5_4[A, B, C, D, E](q: Query_5[A, B, C, D, E]) extends Query_4[A, B, C, D]
  case class RemoveElement1_5[A, B, C, D, E, F](q: Query_6[A, B, C, D, E, F]) extends Query_5[B, C, D, E, F]
  case class RemoveElement2_5[A, B, C, D, E, F](q: Query_6[A, B, C, D, E, F]) extends Query_5[A, C, D, E, F]
  case class RemoveElement3_5[A, B, C, D, E, F](q: Query_6[A, B, C, D, E, F]) extends Query_5[A, B, D, E, F]
  case class RemoveElement4_5[A, B, C, D, E, F](q: Query_6[A, B, C, D, E, F]) extends Query_5[A, B, C, E, F]
  case class RemoveElement5_5[A, B, C, D, E, F](q: Query_6[A, B, C, D, E, F]) extends Query_5[A, B, C, D, F]
  case class RemoveElement6_5[A, B, C, D, E, F](q: Query_6[A, B, C, D, E, F]) extends Query_5[A, B, C, D, E]

  case class SelfJoin11_2[A](q: Query_1[A]) extends Query_2[A, A]
  case class SelfJoin22_4[A, B](q: Query_2[A, B]) extends Query_4[A, B, A, B]
  case class SelfJoin33_6[A, B, C](q: Query_3[A, B, C]) extends Query_6[A, B, C, A, B, C]

  case class Join11_2[A, B](q1: Query_1[A], q2: Query_1[B]) extends Query_2[A, B]
  case class Join12_3[A, B, C](q1: Query_1[A], q2: Query_2[B, C]) extends Query_3[A, B, C]
  case class Join21_3[A, B, C](q1: Query_2[A, B], q2: Query_1[C]) extends Query_3[A, B, C]
  case class Join13_4[A, B, C, D](q1: Query_1[A], q2: Query_3[B, C, D]) extends Query_4[A, B, C, D]
  case class Join22_4[A, B, C, D](q1: Query_2[A, B], q2: Query_2[C, D]) extends Query_4[A, B, C, D]
  case class Join31_4[A, B, C, D](q1: Query_3[A, B, C], q2: Query_1[D]) extends Query_4[A, B, C, D]
  case class Join14_5[A, B, C, D, E](q1: Query_1[A], q2: Query_4[B, C, D, E]) extends Query_5[A, B, C, D, E]
  case class Join23_5[A, B, C, D, E](q1: Query_2[A, B], q2: Query_3[C, D, E]) extends Query_5[A, B, C, D, E]
  case class Join32_5[A, B, C, D, E](q1: Query_3[A, B, C], q2: Query_2[D, E]) extends Query_5[A, B, C, D, E]
  case class Join41_5[A, B, C, D, E](q1: Query_4[A, B, C, D], q2: Query_1[E]) extends Query_5[A, B, C, D, E]
  case class Join15_6[A, B, C, D, E, F](q1: Query_1[A], q2: Query_5[B, C, D, E, F]) extends Query_6[A, B, C, D, E, F]
  case class Join24_6[A, B, C, D, E, F](q1: Query_2[A, B], q2: Query_4[C, D, E, F]) extends Query_6[A, B, C, D, E, F]
  case class Join33_6[A, B, C, D, E, F](q1: Query_3[A, B, C], q2: Query_3[D, E, F]) extends Query_6[A, B, C, D, E, F]
  case class Join42_6[A, B, C, D, E, F](q1: Query_4[A, B, C, D], q2: Query_2[E, F]) extends Query_6[A, B, C, D, E, F]
  case class Join51_6[A, B, C, D, E, F](q1: Query_5[A, B, C, D, E], q2: Query_1[F]) extends Query_6[A, B, C, D, E, F]

  /********************************************************************************************************************/
  /* DSL                                                                                                              */
  /********************************************************************************************************************/

  def stream [A]                (name: String): Query_1[A] =                Stream_1(name)
  def stream [A, B]             (name: String): Query_2[A, B] =             Stream_2(name)
  def stream [A, B, C]          (name: String): Query_3[A, B, C] =          Stream_3(name)
  def stream [A, B, C, D]       (name: String): Query_4[A, B, C, D] =       Stream_4(name)
  def stream [A, B, C, D, E]    (name: String): Query_5[A, B, C, D, E] =    Stream_5(name)
  def stream [A, B, C, D, E, F] (name: String): Query_6[A, B, C, D, E, F] = Stream_6(name)

  case class Query1Helper[A](q: Query_1[A]) {
    def keepEventsWith:                                    Query_1[A] =                KeepEventsWith_1(q)
    def selfJoin:                                          Query_2[A, A] =             SelfJoin11_2(q)
    def join [B]             (q2: Query_1[B]):             Query_2[A, B] =             Join11_2(q, q2)
    def join [B, C]          (q2: Query_2[B, C]):          Query_3[A, B, C] =          Join12_3(q, q2)
    def join [B, C, D]       (q2: Query_3[B, C, D]):       Query_4[A, B, C, D] =       Join13_4(q, q2)
    def join [B, C, D, E]    (q2: Query_4[B, C, D, E]):    Query_5[A, B, C, D, E] =    Join14_5(q, q2)
    def join [B, C, D, E, F] (q2: Query_5[B, C, D, E, F]): Query_6[A, B, C, D, E, F] = Join15_6(q, q2)
  }

  case class Query2Helper[A, B](q: Query_2[A, B]) {
    def keepEventsWith:                              Query_2[A, B] =             KeepEventsWith_2(q)
    def removeElement1:                              Query_1[B] =                RemoveElement1_1(q)
    def removeElement2:                              Query_1[A] =                RemoveElement2_1(q)
    def selfJoin:                                    Query_4[A, B, A, B] =       SelfJoin22_4(q)
    def join [C]          (q2: Query_1[C]):          Query_3[A, B, C] =          Join21_3(q, q2)
    def join [C, D]       (q2: Query_2[C, D]):       Query_4[A, B, C, D] =       Join22_4(q, q2)
    def join [C, D, E]    (q2: Query_3[C, D, E]):    Query_5[A, B, C, D, E] =    Join23_5(q, q2)
    def join [C, D, E, F] (q2: Query_4[C, D, E, F]): Query_6[A, B, C, D, E, F] = Join24_6(q, q2)
  }

  case class Query3Helper[A, B, C](q: Query_3[A, B, C]) {
    def keepEventsWith:                         Query_3[A, B, C] =          KeepEventsWith_3(q)
    def removeElement1:                         Query_2[B, C] =             RemoveElement1_2(q)
    def removeElement2:                         Query_2[A, C] =             RemoveElement2_2(q)
    def removeElement3:                         Query_2[A, B] =             RemoveElement3_2(q)
    def selfJoin:                               Query_6[A, B, C, A, B, C] = SelfJoin33_6(q)
    def join [D]        (q2: Query_1[D]):       Query_4[A, B, C, D] =       Join31_4(q, q2)
    def join [D, E]     (q2: Query_2[D, E]):    Query_5[A, B, C, D, E] =    Join32_5(q, q2)
    def join [D, E, F]  (q2: Query_3[D, E, F]): Query_6[A, B, C, D, E, F] = Join33_6(q, q2)
  }

  case class Query4Helper[A, B, C, D](q: Query_4[A, B, C, D]) {
    def keepEventsWith:                      Query_4[A, B, C, D] =       KeepEventsWith_4(q)
    def removeElement1:                      Query_3[B, C, D] =          RemoveElement1_3(q)
    def removeElement2:                      Query_3[A, C, D] =          RemoveElement2_3(q)
    def removeElement3:                      Query_3[A, B, D] =          RemoveElement3_3(q)
    def removeElement4:                      Query_3[A, B, C] =          RemoveElement4_3(q)
    def join [E]        (q2: Query_1[E]):    Query_5[A, B, C, D, E] =    Join41_5(q, q2)
    def join [E, F]     (q2: Query_2[E, F]): Query_6[A, B, C, D, E, F] = Join42_6(q, q2)
  }

  case class Query5Helper[A, B, C, D, E](q: Query_5[A, B, C, D, E]) {
    def keepEventsWith:                   Query_5[A, B, C, D, E] =    KeepEventsWith_5(q)
    def removeElement1:                   Query_4[B, C, D, E] =       RemoveElement1_4(q)
    def removeElement2:                   Query_4[A, C, D, E] =       RemoveElement2_4(q)
    def removeElement3:                   Query_4[A, B, D, E] =       RemoveElement3_4(q)
    def removeElement4:                   Query_4[A, B, C, E] =       RemoveElement4_4(q)
    def removeElement5:                   Query_4[A, B, C, D] =       RemoveElement5_4(q)
    def join [F]        (q2: Query_1[F]): Query_6[A, B, C, D, E, F] = Join51_6(q, q2)
  }

  case class Query6Helper[A, B, C, D, E, F](q: Query_6[A, B, C, D, E, F]) {
    def keepEventsWith: Query_6[A, B, C, D, E, F] = KeepEventsWith_6(q)
    def removeElement1: Query_5[B, C, D, E, F]    = RemoveElement1_5(q)
    def removeElement2: Query_5[A, C, D, E, F]    = RemoveElement2_5(q)
    def removeElement3: Query_5[A, B, D, E, F]    = RemoveElement3_5(q)
    def removeElement4: Query_5[A, B, C, E, F]    = RemoveElement4_5(q)
    def removeElement5: Query_5[A, B, C, D, F]    = RemoveElement5_5(q)
    def removeElement6: Query_5[A, B, C, D, E]    = RemoveElement6_5(q)
  }

  implicit def query1ToQuery1Helper [A]                (q: Query_1[A]):                Query1Helper[A] =                Query1Helper(q)
  implicit def query2ToQuery2Helper [A, B]             (q: Query_2[A, B]):             Query2Helper[A, B] =             Query2Helper(q)
  implicit def query3ToQuery3Helper [A, B, C]          (q: Query_3[A, B, C]):          Query3Helper[A, B, C] =          Query3Helper(q)
  implicit def query4ToQuery4Helper [A, B, C, D]       (q: Query_4[A, B, C, D]):       Query4Helper[A, B, C, D] =       Query4Helper(q)
  implicit def query5ToQuery5Helper [A, B, C, D, E]    (q: Query_5[A, B, C, D, E]):    Query5Helper[A, B, C, D, E] =    Query5Helper(q)
  implicit def query6ToQuery6Helper [A, B, C, D, E, F] (q: Query_6[A, B, C ,D, E, F]): Query6Helper[A, B, C, D, E, F] = Query6Helper(q)

  /********************************************************************************************************************/
  /* Demo                                                                                                             */
  /********************************************************************************************************************/

  val streamA: Query_2[Int, String] = stream[Int, String]("A")
  val streamB: Query_1[Boolean] = stream[Boolean]("B")

  val query: Query_5[Int, String, Int, String, Boolean] =
    streamA
      .join(streamB)
      .selfJoin
      .removeElement3

}
