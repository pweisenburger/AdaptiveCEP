package adaptivecep.distributed

object Stage extends Enumeration {
  type Stage = Value
  val TentativeOperatorSelection, Measurement, Migration  = Value
}