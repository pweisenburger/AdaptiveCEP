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

package com.scalarookie.eventscala.demos

import com.espertech.esper.client._
import com.scalarookie.eventscala.dsl._
import com.scalarookie.eventscala.backend.esper._
import scala.util.Random
import scala.beans.BeanProperty

/**********************************************************************************************************************/
/* Sample classes representing event types                                                                            */
/* Note: The `@BeanProperty var` part is required by Esper, which is actually a Java library.                         */
/**********************************************************************************************************************/

case class A(@BeanProperty var id: Int)
case class B(@BeanProperty var id: Int)
case class C(@BeanProperty var id: Int)

/**********************************************************************************************************************/
/* `update` defines what happens when an event is received as a result of the query to which the `Listener` is added. */
/**********************************************************************************************************************/

class Listener extends UpdateListener {
  override def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) =
    println("Event received: " + newEvents(0).getUnderlying)
}

object Esper extends App {

  /********************************************************************************************************************/
  /* `emitEvents` continuously emits instances of the classes `A`, `B`, and `C` as events.                            */
  /********************************************************************************************************************/

  def emitEvents(runtime: EPRuntime, aId: Int = 0, bId: Int = 0, cId: Int = 0): Unit = {
    val whichClazz = Random.nextInt(3)
    val event = whichClazz match {
      case 0 => new A(aId)
      case 1 => new B(bId)
      case 2 => new C(cId)
    }
    println("Event emitted: " + event)
    runtime.sendEvent(event)
    Thread.sleep(Random.nextInt(1000))
    whichClazz match {
      case 0 => emitEvents(runtime, aId + 1, bId, cId)
      case 1 => emitEvents(runtime, aId, bId + 1, cId)
      case 2 => emitEvents(runtime, aId, bId, cId + 1)
    }
  }

  /********************************************************************************************************************/
  /* Esper boilerplate code -.-                                                                                       */
  /********************************************************************************************************************/

  val configuration = new Configuration
  configuration.addEventType(classOf[A])
  configuration.addEventType(classOf[B])
  configuration.addEventType(classOf[C])

  val serviceProvider = EPServiceProviderManager.getProvider("ServiceProvider", configuration)
  val runtime = serviceProvider.getEPRuntime
  val administrator = serviceProvider.getEPAdministrator

  /********************************************************************************************************************/
  /* Usage of EventScala                                                                                              */
  /* Note: First, for all classes that are to be used in a query, we comfortably get a companion object of the same   */
  /*       name to which we will refer to instead, using `@GetCompanionObject`. Then, the query is being expressed    */
  /*       using EventScala's syntax. The type of `query` is `Query`, which is an instance of the most outer one of   */
  /*       many nested case classes, which are used internally to represent the query. From this case class           */
  /*       representation, SQL-like strings for almost every event processing engine could be generated. By not just  */
  /*       importing `com.scalarookie.eventscala.dsl._` but also `com.scalarookie.eventscala.backend.esper._`, we get */
  /*       this functionality for Esper. There is a function `query2EplString` in scope, which takes a `Query` and    */
  /*       returns a `String`, i.e., the query as an EPL string. One could explicitly call it, but since it is        */
  /*       `implicit`, it is possible to just pass `query` to `createEPL`. The latter expects a `String`, and         */
  /*       `query2EplString` will implicitly provide one, given a `Query`.                                            */
  /********************************************************************************************************************/

  @GetCompanionObject[A] object A
  @GetCompanionObject[B] object B
  @GetCompanionObject[C] object C

  val query = select (A.id, B.id, C.id) { A() -> B() -> C() }

  val statement = administrator.createEPL(query)
  statement.addListener(new Listener)

  /********************************************************************************************************************/
  /* Below find some more queries expressed in EventScala's syntax. Uncomment and paste above to try them out.        */
  /* Note: These are taken straight from the unit tests. There you can also take a look at the internal case class    */
  /*       representation mentioned above.                                                                            */
  /********************************************************************************************************************/

  // Primitive representing a stream of instances of a class - No filter
  // val query = select () { A() }

  // Primitive representing a stream of instances of a class - Filter 1
  // val query = select () { A(A.id === 10) }

  // Primitive representing a stream of instances of a class - Filter 2
  // val query = select () { A(A.id < 10) }

  // Primitive representing a stream of instances of a class - Filter 3
  // val query = select () { A(A.id > 10) }

  // Primitive representing a stream of instances of time - AtEvery
  // val query = select () { AtEvery(** h ** m 30 s) }

  // Primitive representing a stream of instances of time - AfterEvery
  // val query = select () { AfterEvery(30 s) }

  // Operator - Sequence
  // val query = select () { A() -> B() }

  // Operator - And
  // val query = select () { A() & B() }

  // Operator - Or
  // val query = select () { A() | B() }

  // Operator - Not
  // val query = select () { !A() }

  // Nested - 1
  // val query = select (A.id, B.id, C.id) { A() -> (B() -> C()) }

  // Nested - 2
  // val query = select (A.id, B.id) { AfterEvery(3 s) -> (A() | B()) }

  // Nested - 3
  // val query = select (A.id, B.id) { AfterEvery(1 s) & !(A() | B()) }

  // Nested - 4
  // val query = select (A.id, B.id) { AtEvery(** h ** m 15 s) -> (AfterEvery(1 s) & !A() | B()) }

  emitEvents(runtime)

}
