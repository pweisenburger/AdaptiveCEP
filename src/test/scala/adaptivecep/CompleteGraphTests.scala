package adaptivecep

import org.scalatest.Sequential

class CompleteGraphTests extends Sequential(
  new BasicGraphTests,
  new RecordGraphTests,
  new EmptyQueryGraphTests,
  new TupleHListInteractionGraphTests
)