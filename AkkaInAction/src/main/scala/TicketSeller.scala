import akka.actor.{Actor, PoisonPill, Props}

object TicketSeller {
  def props(event: String) = Props(new TicketSeller(event))

  case class Add(tickets: Vector[Ticket]) // Message to add tickets to the TicketSeller
  case class Buy(tickets: Int)            // Message to buy tickets from the TicketSeller
  case class Ticket(id: Int)              // A ticket
  case class Tickets(                     // A list of tickets for an event
    event: String,
    entries: Vector[Ticket] = Vector.empty[Ticket])
  case object GetEvent                    // A message containing the remaining tickets for the event
  case object Cancel                      // A message to cancel the event
}

class TicketSeller(event: String) extends Actor {
  import TicketSeller._

  var tickets = Vector.empty[Ticket]

  def receive = {
    case Add(newTickets) =>
      tickets = tickets ++ newTickets

    case Buy(nrOfTickets) =>
      val entries = tickets.take(nrOfTickets).toVector
      if (entries.size >= nrOfTickets) {
        sender() ! Tickets(event, entries)
      } else {
        sender() ! Tickets(event)
      }

    case GetEvent =>
      sender() ! Some(BoxOffice.Event(event, tickets.size))

    case Cancel =>
      sender() ! Some(BoxOffice.Event(event, tickets.size))
      self ! PoisonPill
  }
}
