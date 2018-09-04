package adaptivecep.simulation

import java.io.{File, PrintStream}

import adaptivecep.data.Queries.IQuery

object SimulationRunner extends App {
  val directory =
    args.headOption map { new File(_) } flatMap { directory =>
      if (!directory.isDirectory) {
        System.err.println(s"$directory does not exist or is not a directory")
        None
      }
      else
        Some(directory)
    }

  println("Starting simulation")

  SimulationSetup.queries foreach { case (name, query) =>
    runSimulation(query)(directory, s"$name-latency-everymin") { (simulation, timeSeconds, latencyMillis, bandwidth) =>
      if ((timeSeconds % 60) == 0 && latencyMillis > 80)
        simulation.placeOptimizingLatency()
    }

    runSimulation(query)(directory, s"$name-latency-everysec") { (simulation, timeSeconds, latencyMillis, bandwidth) =>
      if (latencyMillis > 80)
        simulation.placeOptimizingLatency()
    }

    runSimulation(query)(directory, s"$name-bandwidth-everymin") { (simulation, timeSeconds, latencyMillis, bandwidth) =>
      if ((timeSeconds % 60) == 0 && bandwidth < 25)
        simulation.placeOptimizingBandwidth()
    }

    runSimulation(query)(directory, s"$name-bandwidth-everysec") { (simulation, timeSeconds, latencyMillis, bandwidth) =>
      if (bandwidth < 40)
        simulation.placeOptimizingBandwidth()
    }

    runSimulation(query)(directory, s"$name-latencybandwidth-everymin") { (simulation, timeSeconds, latencyMillis, bandwidth) =>
      if ((timeSeconds % 60) == 0 && (latencyMillis > 80 || bandwidth < 25))
        simulation.placeOptimizingLatencyAndBandwidth()
    }

    runSimulation(query)(directory, s"$name-latencybandwidth-everysec") { (simulation, timeSeconds, latencyMillis, bandwidth) =>
      if (latencyMillis > 80 || bandwidth < 25)
        simulation.placeOptimizingLatencyAndBandwidth()
    }
  }

  println("Simulation finished")
}

object runSimulation {
  def apply(queries: IQuery*)(directory: Option[File], name: String)(optimize: (Simulation, Long, Long, Long) => Unit) = {
    println(s"Simulating $name")

    val out = directory map { directory => new PrintStream(new File(directory, s"$name.csv")) } getOrElse System.out
    out.println(name)
    new SimulationSetup(queries: _*)(out)(optimize).run()
    directory foreach { _ => out.close() }
  }
}
