import scala.concurrent.Future
import akka.actor.ActorSystem
import akka.event.Logging
import akka.util.Timeout
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer
import com.typesafe.config.{ Config, ConfigFactory }

object Main extends App with RequestTimeout {
  val config = ConfigFactory.load()
  val host = config.getString("http.host")
  val port = config.getInt("http.port")

  implicit val system = ActorSystem()
  implicit val ec = system.dispatcher

  val api = new RestApi(system, requestTimeout(config)).routes

  implicit val materializer = ActorMaterializer()

  val bindingFuture: Future[ServerBinding] = Http().bindAndHandle(api, host, port)

  val log = Logging(system.eventStream, "go-ticks")
  bindingFuture.map { serverBinding =>
    log.info(s"RestApi bound to ${serverBinding.localAddress}.")
  }.onFailure {
    case ex: Exception =>
      log.error(ex, "Failed to bind to {}:{}!", host, port)
      system.terminate()
  }
}

trait RequestTimeout {
  import scala.concurrent.duration._

  def requestTimeout(config: Config): Timeout = {
    val t = config.getString("akka.http.server.request-timeout")
    val d = Duration(t)
    FiniteDuration(d.length, d.unit)
  }
}
