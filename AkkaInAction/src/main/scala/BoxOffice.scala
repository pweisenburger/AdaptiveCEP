import akka.actor.{Actor, ActorRef}

object BoxOffice {
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

class BoxOffice extends Actor {
  import BoxOffice._

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
  }
}
