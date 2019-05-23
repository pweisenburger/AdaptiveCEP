package adaptivecep.distributed

import java.io.File

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

object HostSystem{

  def main(args: Array[String]): Unit = {
    if (args.isEmpty)
      startup(Seq("application.conf"))
    else
      startup(args)
  }

  def startup(args: Seq[String]): Unit = {
      val file = new File(/*"fixedHosts/" + */args.head + ".conf")
      val config = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve()

      val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)
  }
}
