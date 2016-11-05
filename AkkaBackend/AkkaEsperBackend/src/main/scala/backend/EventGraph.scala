package backend

import akka.actor.{Actor, ActorRef, Props}
import backend.EventGraph.Created
import com.espertech.esper.client.{EventBean, UpdateListener}
import middleend._
import backend.EventPublisher._

object EventGraph {

  case class Created(nodeId: String, eplString: String)

}

class EventGraph(
    query: EventQuery,
    primitiveTypes: Map[String, Class[_]],
    primitivePublishers: Map[String, ActorRef],
    rootReceiver: Option[ActorRef] = None)
  extends Actor with EsperInstance {

  val nodeId: String = self.path.name
  val receiver: ActorRef = rootReceiver.getOrElse(context.parent)
  val childNodes: Map[String, Option[ActorRef]] = createChildNodes

  // Register primitive types contained in `query` with Esper
  getPrimitiveNames.foreach(name => { println(s"$nodeId registered for $name."); registerEventType(name, primitiveTypes(name)) })

  // Get an EPL string representing `query` and an `UpdateListener` for it and register both with Esper
  registerEplStringWithUpdateListener(createEplString, createUpdateListener)

  // Subscribe to respective publisher for each primitive type contained in `query`
  getPrimitiveNames.foreach(name => primitivePublishers(name) ! Subscribe)

  // TODO: For debugging purposes only!
  println(s"\nNode $nodeId created.\nQuery:\t$query\nEPL:\t$createEplString\n")

  def receive = {
    case event => runtime.sendEvent(event)
  }

  def getPrimitiveNames: Set[String] = {
    def getPrimitiveNamesFromSubquery(subquery: EventQuery): Set[String] = subquery match {
      case PrimitiveQuery(name) => Set(name)
      case _ => Set.empty
    }
    query match {
      case PrimitiveQuery(name) => Set(name)
      case SequenceQuery(qs) => qs.map(getPrimitiveNamesFromSubquery).toSet.flatten
      case AndQuery(q0, q1) => getPrimitiveNamesFromSubquery(q0) ++ getPrimitiveNamesFromSubquery(q1)
      case OrQuery(q0, q1) => getPrimitiveNamesFromSubquery(q0) ++ getPrimitiveNamesFromSubquery(q1)
    }
  }

  def createEplString: String = {
    def createEplStringFromSubquery(subquery: EventQuery, subqueryId: Int): String = subquery match {
      case PrimitiveQuery(name) => s"q$subqueryId=$name"
      case SequenceQuery(_) => s"q$subqueryId=SequenceInstance(nodeId='$nodeId-q$subqueryId')"
      case AndQuery(_, _) => s"q$subqueryId=AndInstance(nodeId='$nodeId-q$subqueryId')"
      case OrQuery(_, _) => s"q$subqueryId=OrInstance(nodeId='$nodeId-q$subqueryId')"
    }
    query match {
      case PrimitiveQuery(name) =>
        s"select * from pattern [every (q0=$name)]"
      case SequenceQuery(qs) =>
        val qsEplStrings = qs.zipWithIndex.map { case (q, i) => createEplStringFromSubquery(q, i) }
        s"select * from pattern [every (${qsEplStrings.mkString(" -> ")})]"
      case AndQuery(q0, q1) =>
        val q0EplString = createEplStringFromSubquery(q0, 0)
        val q1EplString = createEplStringFromSubquery(q1, 1)
        s"select * from pattern [every ($q0EplString and $q1EplString)]"
      case OrQuery(q0, q1) =>
        val q0EplString = createEplStringFromSubquery(q0, 0)
        val q1EplString = createEplStringFromSubquery(q1, 1)
        s"select * from pattern [every ($q0EplString or $q1EplString)]"
    }
  }

  def createUpdateListener: UpdateListener = {
    def castInstance(instanceQuery: EventQuery, instanceObject: Object): EventInstance = instanceQuery match {
      case PrimitiveQuery(name) => PrimitiveInstance(primitiveTypes(name).cast(instanceObject), nodeId)
      case SequenceQuery(_) => instanceObject.asInstanceOf[SequenceInstance]
      case AndQuery(_, _) => instanceObject.asInstanceOf[AndInstance]
      case OrQuery(_, _) => instanceObject.asInstanceOf[OrInstance]
    }
    query match {
      case PrimitiveQuery(name) => new UpdateListener {
        def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) = {
          val i0 = primitiveTypes(name).cast(newEvents(0).get("q0"))
          receiver ! PrimitiveInstance(i0, nodeId)
        }
      }
      case SequenceQuery(qs) => new UpdateListener {
        def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) = {
          val is = qs.zipWithIndex.map { case (q, i) => castInstance(q, newEvents(0).get(s"q$i")) }
          receiver ! SequenceInstance(is, nodeId)
        }
      }
      case AndQuery(q0, q1) => new UpdateListener {
        def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) = {
          val i0 = castInstance(q0, newEvents(0).get("q0"))
          val i1 = castInstance(q1, newEvents(0).get("q1"))
          receiver ! AndInstance(i0, i1, nodeId)
        }
      }
      case OrQuery(q0, q1) => new UpdateListener {
        def update(newEvents: Array[EventBean], oldEvents: Array[EventBean]) = newEvents(0).get("q0") match {
          case null => receiver ! OrInstance(None, Some(castInstance(q1, newEvents(0).get("q1"))), nodeId)
          case obj => receiver ! OrInstance(Some(castInstance(q0, obj)), None, nodeId)
        }
      }
    }
  }

  def createChildNodes: Map[String, Option[ActorRef]] = {
    def createChildNodeIfNotPrimitive(subquery: EventQuery, childNodeId: String): Option[ActorRef] = subquery match {
      case PrimitiveQuery(_) => None
      case _ => Some(context.actorOf(Props(new EventGraph(subquery, primitiveTypes, primitivePublishers)), childNodeId))
    }
    query match {
      case PrimitiveQuery(_) => Map.empty
      case SequenceQuery(qs) => qs.zipWithIndex.map { case (q, i) =>
        s"q$i" -> createChildNodeIfNotPrimitive(q, s"$nodeId-q$i")
      }.toMap
      case AndQuery(q0, q1) => Map(
        "q0" -> createChildNodeIfNotPrimitive(q0, s"$nodeId-q0"),
        "q1" -> createChildNodeIfNotPrimitive(q1, s"$nodeId-q1")
      )
      case OrQuery(q0, q1) => Map(
        "q0" -> createChildNodeIfNotPrimitive(q0, s"$nodeId-q0"),
        "q1" -> createChildNodeIfNotPrimitive(q1, s"$nodeId-q1")
      )
    }
  }

}
