package adaptivecep.simulation

import java.io.{ByteArrayOutputStream, File, FileInputStream, InputStream}
import java.net.InetSocketAddress

import adaptivecep.data.Events.Init
import akka.actor.{Actor, ActorRef}
import akka.io.{IO, Udp}
import akka.util.ByteString

class UpdateGeneratorRemote extends Actor {

  import context.system
  //add hostname and port of the Listener
  val remote = new InetSocketAddress("", 0)

  // add own hostname and own port
  IO(Udp) ! Udp.Bind(self, new InetSocketAddress("", 0))

  var classData: Array[Byte] = Array.emptyByteArray


  def receive = {
    case Udp.Bound(_) =>
      context.become(ready(sender()))
  }

  def ready(send: ActorRef): Receive = {
    case Init =>
      //add path to .class file
      classData = readByteData(new FileInputStream(new File("")))
      val bytestring = ByteString.fromArray(classData)

      // add full name of the class to update
      send ! Udp.Send(ByteString(""), remote)
      send ! Udp.Send(bytestring, remote)
  }

  def readByteData(in: InputStream): Array[Byte] = {
    var baos: ByteArrayOutputStream = null
    var data: Array[Byte] = null
    var n: Int = 0

    data = new Array[Byte](8192)
    baos = new ByteArrayOutputStream

    n = in.read(data, 0, 8192)
    while (n > -1) {
      baos.write(data, 0, n)
      n = in.read(data, 0, 8192)
    }

    baos.toByteArray
  }

}
