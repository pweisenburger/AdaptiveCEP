import spray.json._

// Message containing the initial number of tickets for the event
case class EventDescription(tickets: Int) {
  require(tickets > 0)
}

// Message containing the required number of tickets
case class TicketRequest(tickets: Int) {
  require(tickets > 0)
}

// Message containing an error
case class Error(message: String)

trait EventMarshalling extends DefaultJsonProtocol {
  import BoxOffice._

  implicit val eventDescriptionFormat = jsonFormat1(EventDescription)
  implicit val eventFormat = jsonFormat2(Event)
  implicit val eventsFormat = jsonFormat1(Events)
  implicit val ticketRequestFormat = jsonFormat1(TicketRequest)
  implicit val ticketFormat = jsonFormat1(TicketSeller.Ticket)
  implicit val ticketsFormat = jsonFormat2(TicketSeller.Tickets)
  implicit val errorFormat = jsonFormat1(Error)
}
