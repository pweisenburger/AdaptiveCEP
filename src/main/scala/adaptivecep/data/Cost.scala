package adaptivecep.data

import scala.concurrent.duration.Duration

object Cost{
  case class Cost(duration: Duration, bandwidth: Double)
}
