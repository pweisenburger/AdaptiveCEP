package distributedadaptivecep.simulation

import adaptivecep.data.Queries.Query
import adaptivecep.dsl.Dsl._

/**
  * Created by pratik_k on 7/18/2017.
  */
object SimulationQueries {
  val query0: Query =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .where(_ <= _)
      .removeElement1()

  val query1: Query =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .where(_ <= _)
      .join(
        stream[Int]("C")
          .where(0 <= _),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .removeElement1()

  val queryDepth4: Query =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .where(_ <= _)
      .join(
        stream[Int]("C"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .removeElement1()

  val queryDepth5: Query =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .where(_ <= _)
      .join(
        stream[Int]("C"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .join(
        stream[Int]("D"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .removeElement1()

  val queryDepth6: Query =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .where(_ <= _)
      .join(
        stream[Int]("C"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .join(
        stream[Int]("D"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .join(
        stream[Int]("E"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .removeElement1()

  val queryDepth7: Query =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .where(_ <= _)
      .join(
        stream[Int]("C"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .where(_ <= _ + _)
      .join(
        stream[Int]("D"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .join(
        stream[Int]("E"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .removeElement1()

  val queryDepth8: Query =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .where(_ <= _)
      .join(
        stream[Int]("C"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .join(
        stream[Int]("D"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .join(
        stream[Int]("E"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .join(
        stream[Int]("A"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .removeElement1()
	  
  val depth5: Query =
	stream[Int]("A")
	  .join(
		  stream[Int]("B"),
		  slidingWindow(30.seconds),
		  slidingWindow(30.seconds))
	  .where { (a, b) => a < b }
	  .where { (a, b) => a * b > 20 }
	  .join(
		  stream[Int]("C"),
		  slidingWindow(30.seconds),
		  slidingWindow(30.seconds))
	  .removeElement1()

  val depth6: Query =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .where { (a, b) => a < b }
      .where { (a, b) => a * b > 20 }
      .where { (a, b) => a * a < 200 }
      .join(
        stream[Int]("C"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .removeElement1()

  val depth7: Query =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .where { (a, b) => a < b }
      .where { (a, b) => a * b > 20 }
      .where { (a, b) => a * a < 200 }
      .where { (a, b) => b * b < a }
      .join(
        stream[Int]("C"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .removeElement1()

  val sourcesQuery3: Query =stream[Int]("A")
    .join(
      stream[Int]("B")
        join(stream[Int]("C"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds)),
      slidingWindow(30.seconds),
      slidingWindow(30.seconds))

  val sourcesQuery4: Query =stream[Int]("A")
    .join(
      stream[Int]("B")
        join(stream[Int]("C"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds)),
      slidingWindow(30.seconds),
      slidingWindow(30.seconds))
    .join(
      stream[Int]("D"),
      slidingWindow(30.seconds),
      slidingWindow(30.seconds))

  val sourcesQuery5: Query =stream[Int]("A")
    .join(
      stream[Int]("B")
        join(stream[Int]("C"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds)),
      slidingWindow(30.seconds),
      slidingWindow(30.seconds))
    .join(
      stream[Int]("D")
        .join(stream[Int]("E"),
          slidingWindow(30.seconds),
          slidingWindow(30.seconds)),
      slidingWindow(30.seconds),
      slidingWindow(30.seconds))

  val sourcesQuery6: Query =stream[Int]("A")
    .join(
      stream[Int]("B")
        join(stream[Int]("C"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds)),
      slidingWindow(30.seconds),
      slidingWindow(30.seconds))
    .join(
      stream[Int]("D")
        .join(stream[Int]("E")
          .join(stream[Int]("C"),
            slidingWindow(30.seconds),
            slidingWindow(30.seconds)),
          slidingWindow(30.seconds),
          slidingWindow(30.seconds)),
      slidingWindow(30.seconds),
      slidingWindow(30.seconds))

}
