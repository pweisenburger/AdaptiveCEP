package adaptivecep

import adaptivecep.data.Events.Created
import adaptivecep.data.Queries.IQuery
import adaptivecep.graph.factory.GraphFactory
import adaptivecep.graph.qos.DummyMonitorFactory
import adaptivecep.publishers.TestPublisher
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

abstract class GraphTestSuite extends TestKit(ActorSystem()) with FunSuiteLike with BeforeAndAfterAll {

  def createTestPublisher(name: String): ActorRef = {
    system.actorOf(Props(TestPublisher()), name)
  }

  def createTestGraph(query: IQuery, publishers: Map[String, ActorRef], testActor: ActorRef): ActorRef = GraphFactory.createImpl(
    system,
    query,
    publishers,
    DummyMonitorFactory(),
    DummyMonitorFactory(),
    () => testActor ! Created,
    event => testActor ! event)

  // The following method implementation is taken straight out of the Akka docs:
  // http://doc.akka.io/docs/akka/current/scala/testing.html#Watching_Other_Actors_from_Probes
  def stopActor(actor: ActorRef): Unit = {
    val probe = TestProbe()
    probe watch actor
    actor ! PoisonPill
    probe.expectTerminated(actor)
  }

  def stopActors(actors: ActorRef*): Unit = {
    actors.foreach(stopActor)
  }

  override def afterAll(): Unit = {
    system.terminate()
  }
}
