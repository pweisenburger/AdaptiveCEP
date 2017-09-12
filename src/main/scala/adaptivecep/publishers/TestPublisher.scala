package adaptivecep.publishers

import adaptivecep.publishers.Publisher.Subscribe

case class TestPublisher() extends Publisher {

  override def receive: Receive = {
    case Subscribe =>
      super.receive(Subscribe) // TODO Maybe()
    case message =>
      subscribers.foreach(_ ! message)
  }

}
