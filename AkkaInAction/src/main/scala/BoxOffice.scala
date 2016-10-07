import scala.concurrent.Future
import akka.actor._
import akka.util.Timeout

object BoxOffice {
  def props(implicit timeout: Timeout) = Props(new BoxOffice)
  def name = "boxOffice"

  case class CreateEvent(name: String, tickets: Int)          // Message to create an event
  case class GetEvent(name: String)                           // Message to get an event
  case object GetEvents                                       // Message to request all events
  case class GetTickets(event: String, tickets: Int)          // Message to get tickets for an event
  case class CancelEvent(name: String)                        // Message to cancel an event

  case class Event(name: String, tickets: Int)                // Message describing the event
  case class Events(events: Vector[Event])                    // Message to describe a list of events

  sealed trait EventResponse                                  // Message response to CreateEvent
  case class EventCreated(event: Event) extends EventResponse // Message to indicate the event was created
  case object EventExists extends EventResponse               // Message to indicate that the event already exists
}

class BoxOffice(implicit timeout: Timeout) extends Actor {
  import BoxOffice._
  import context._

  def createTicketSeller(name: String) =
    context.actorOf(TicketSeller.props(name), name)

  def receive = {
    case CreateEvent(name, tickets) =>
      def create() = {
        val ticketSeller = createTicketSeller(name)
        val newTickets = (1 to tickets).map { ticketId =>
          TicketSeller.Ticket(ticketId)
        }.toVector
        ticketSeller ! TicketSeller.Add(newTickets)
        sender() ! EventCreated
      }
      context.child(name).fold(create())(_ => sender() ! EventExists)

    case GetTickets(event, tickets) =>
      def notFound() = sender() ! TicketSeller.Tickets(event)
      def buy(child: ActorRef) = child.forward(TicketSeller.Buy(tickets))
      context.child(event).fold(notFound())(buy)

    case GetEvent(event) =>
      def notFound() = sender() ! None
      def getEvent(child: ActorRef) = child forward TicketSeller.GetEvent
      context.child(event).fold(notFound())(getEvent)

    case GetEvents =>
      import akka.pattern.ask
      import akka.pattern.pipe
      // A local method definition for asking all TicketSellers about the events they sell tickets for.
      def getEvents = context.children.map { child =>
        self.ask(GetEvent(child.path.name)).mapTo[Option[Event]]
      }
      // We're going to ask all TicketSellers. Asking GetEvent returns an Option[Event], so when mapping over all
      // TicketSellers we'll end up with an Iterable[Option[Event]]. This method flattens the Iterable[Option[Event]]
      // into a Iterable[Event], leaving out all the empty Option results. The Iterable is transformed into an Events
      // message.
      def convertToEvents(f: Future[Iterable[Option[Event]]]) =
        f.map(_.flatten).map(l => Events(l.toVector))
      // ask returns a Future, at type that will eventually contain a value. getEvents returns
      // Iterable[Future[Option[Event]]]; sequence can turn this into a Future[Iterable[Option[Event]]]. pipe sends the
      // value inside the Future to an actor the moment it's complete, in this case the sender of the GetEvents message,
      // the RestApi.
      pipe(convertToEvents(Future.sequence(getEvents))) to sender()

    case CancelEvent(event) =>
      def notFound() = sender() ! None
      def cancelEvent(child: ActorRef) = child forward TicketSeller.Cancel
      context.child(event).fold(notFound())(cancelEvent)
  }
}
