package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries.{Disjunction, DisjunctionQuery, Query}
import adaptivecep.graph.nodes.traits.BinaryNode
import adaptivecep.graph.qos.MonitorFactory
import akka.actor.ActorRef

case class DisjunctionNode(
    query: DisjunctionQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends BinaryNode {

  var childNode1Created: Boolean = false
  var childNode2Created: Boolean = false

  def fillArray(desiredLength: Int, array: Array[Either[Any, Any]]): Array[Either[Any, Any]] = {
    require(array.length <= desiredLength)
    // need this check because otherwise matching array(0) fails
    if (array.length == 0)
      return Array()

    val unit: Either[Unit, Unit] = array(0) match {
      case Left(_) => Left(())
      case Right(_) => Right(())
    }
    (0 until desiredLength).map(i => {
      if (i < array.length) {
        array(i)
      } else {
        unit
      }
    }).toArray
  }

  def handleEvent(array: Array[Either[Any, Any]]): Unit = {
    val disjunction = query.asInstanceOf[Disjunction[_, _, _]]
    val filledArray: Array[Either[Any, Any]] = fillArray(disjunction.length, array)
    emitEvent(Event(filledArray: _*))
  }

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode1, childNode2))
    case Created if sender() == childNode1 =>
      childNode1Created = true
      if (childNode2Created) emitCreated()
    case Created if sender() == childNode2 =>
      childNode2Created = true
      if (childNode1Created) emitCreated()
    case event: Event if sender() == childNode1 =>
      val array: Array[Either[Any, Any]] = event.es.map { e => Left(e) }.toArray
      handleEvent(array)
    case event: Event if sender() == childNode2 =>
      val array: Array[Either[Any, Any]] = event.es.map { e => Right(e) }.toArray
      handleEvent(array)
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

}
