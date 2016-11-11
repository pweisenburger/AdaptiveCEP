package stream_representation

object StreamRepresentation extends App {

  case class Stream1[A](t: (A))
  case class Stream2[A, B](t: (A, B))
  case class Stream3[A, B, C](t: (A, B, C))
  case class Stream4[A, B, C, D](t: (A, B, C, D))

  def join11[A, B](s1: Stream1[A], s2: Stream1[B]) = Stream2[A, B](s1.t, s2.t)
  def join12[A, B, C](s1: Stream1[A], s2: Stream2[B, C]) = Stream3[A, B, C](s1.t, s2.t._1, s2.t._2)
  def join13[A, B, C, D](s1: Stream1[A], s2: Stream3[B, C, D]) = Stream4[A, B, C, D](s1.t, s2.t._1, s2.t._2, s2.t._3)
  def join21[A, B, C](s1: Stream2[A, B], s2: Stream1[C]) = Stream3[A, B, C](s1.t._1, s1.t._2, s2.t)
  def join22[A, B, C, D](s1: Stream2[A, B], s2: Stream2[C, D]) = Stream4[A, B, C, D](s1.t._1, s1.t._2, s2.t._1, s2.t._2)
  def join31[A, B, C, D](s1: Stream3[A, B, C], s2: Stream1[D]) = Stream4[A, B, C, D](s1.t._1, s1.t._2, s1.t._3, s2.t)

  println(join22(Stream2((13, "Foo")), Stream2((42, "Bar"))) == Stream4((12, "Foo", 42, "Bar")))

}
