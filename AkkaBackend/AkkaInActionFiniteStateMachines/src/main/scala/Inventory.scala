import akka.actor.{Actor, ActorRef, FSM}

// Events
case class BookRequest(context: AnyRef, target: ActorRef)
case class BookSupply(nrBooks: Int)
case object BookSupplySoldOut
case object Done
case object PendingRequests

// Responses
case object PublisherRequest
case class BookReply(context: AnyRef, reserveId: Either[String, Int])

// States
sealed trait State
case object WaitForRequests extends State
case object ProcessRequest extends State
case object WaitForPublisher extends State
case object SoldOut extends State
case object ProcessSoldOut extends State

case class StateData(nrBooksInStore: Int, pendingRequests: Seq[BookRequest])

class Inventory(publisher: ActorRef) extends Actor with FSM[State, StateData] {

  var reserveId = 0

  startWith(WaitForRequests, new StateData(0, Seq()))

  when(WaitForRequests) {
    case Event(request: BookRequest, data: StateData) => {
      val newStateData = data.copy(pendingRequests = data.pendingRequests :+ request)
      if (newStateData.nrBooksInStore > 0)
        goto(ProcessRequest) using newStateData
      else
        goto(WaitForPublisher) using newStateData
    }
    case Event(PendingRequests, data: StateData) => {
      if (data.pendingRequests.isEmpty)
        stay
      else if (data.nrBooksInStore > 0)
        goto(ProcessRequest)
      else
        goto(WaitForPublisher)
    }
  }

  when(WaitForPublisher) {
    case Event(supply: BookSupply, data: StateData) => {
      goto(ProcessRequest) using data.copy(nrBooksInStore = supply.nrBooks)
    }
    case Event(BookSupplySoldOut, _) => {
      goto(ProcessSoldOut)
    }
  }

  // TODO Continue from here!

  when(ProcessRequest) {
    case Event(Done, data: StateData) => {
      ???
    }
  }

  when(SoldOut) {
    case Event(request: BookRequest, data: StateData) => {
      ???
    }
  }

  when(ProcessSoldOut) {
    case Event(Done, data: StateData) => {
      ???
    }
  }

  whenUnhandled {
    case Event(request: BookRequest, data: StateData) => {
      ???
    }
    case Event(e, s) => {
      ???
    }
  }

  initialize

}