package learning_typetags_and_classtags

object Examples extends App {

  // -------------------------------------
  // Learning Scala TypeTags and ClassTags
  // by Lucas Bärenfänger (@scalarookie)
  // -------------------------------------

  // ------------------------------------------------------------------------------
  // Monsanto example (http://engineering.monsanto.com/2015/05/14/implicits-intro/)
  // ------------------------------------------------------------------------------

  import scala.reflect.runtime.universe._

  def getInnerType[T](list: List[T])(implicit tag: TypeTag[T]) =
    tag.tpe.toString

  // println(getInnerType(List(1, 2, 3)))
  // println(getInnerType(List("Hi")))

  // ----------------------------------------------------------------------------------------------------------------
  // Medium article example (https://medium.com/@sinisalouc/overcoming-type-erasure-in-scala-8f2422070d20#.jhhrfedrm)
  // ----------------------------------------------------------------------------------------------------------------

  def extract[T](list: List[Any]) = list.flatMap {
    // This doesn't work due to type erasure, as `T` becomes `Object`?
    case element: T => Some(element)
    case _ => None
  }

  // Incorrectly prints "List(Hi, 1)" to the console.
  // println(extract[Int](List("Hi", 1)))

  import scala.reflect.ClassTag

  def extractUsingCT[T](list: List[Any])(implicit tag: ClassTag[T]) = list.flatMap {
    case element: T => Some(element)
    case _ => None
  }

  // Correctly prints "List(Hi)" to the console, doesn't work with `Int` as type argument, though. -.-
  // println(extractUsingCT[String](List("Hi", 1)))

  // Here's what the above function gets translated into by the compiler:
  def extractUsingCT2[T](list: List[Any])(implicit tag: ClassTag[T]) = list.flatMap {
    case (element @ tag(_: T)) => Some(element)
    case _ => None
  }

  // Shorthand notation:
  def extractUsingCT3[T : ClassTag](list: List[Any]) = list.flatMap {
    case element: T => Some(element)
    case _ => None
  }

  // Key weakness of ClassTags: "ClassTags cannot differentiate on a higher level." This will not work as expected:
  // println(extractUsingCT[List[String]](List(List("Hi"), List(1))))

  // With TypeTags, "[w]e can easily get the full path of the type in question."
  // "To get this information, you just need to invoke tpe() on a given tag."

  def myOwnRecognize[T](t: T)(implicit tag: TypeTag[T]) =
    tag.tpe.toString

  // Prints "List[java.lang.String]" to the console.
  // println(myOwnRecognize(List("Hi")))

  // Here’s an example. [...] Pay attention to the “args” argument — it’s the one that contains additional type
  // information which ClassTag doesn’t have (information about List being parametrized by Int).

  def recognize[T](x: T)(implicit tag: TypeTag[T]) = tag.tpe match {
    case TypeRef(utype, usymbol, args) =>
      s"utype: $utype, usymbol: $usymbol, args: $args"
  }

  val listInt: List[List[Int]] = List(List(1))
  val listAny: List[List[Any]] = List(List(1))

  // Prints "utype: scala.type, usymbol: type List, args: List(scala.List[Int])" to the console.
  // println(recognize(listInt))
  // Prints "utype: scala.type, usymbol: type List, args: List(scala.List[Any])" to the console.
  // println(recognize(listAny))

  // Downside of TypeTags:
  // "We cannot implement an Extractor using TypeTags. Good thing about them is having more information about the type,
  //  such as knowing about the higher types (that is, being able to differentiate List[X] from List[Y]), but their
  //  downside is that they cannot be used on objects at runtime. We can use the TypeTag to get information about a
  //  certain type at runtime, but we cannot use it to find out the type of some object at runtime. Do you see the
  //  difference? What we passed to recognize() was a straightforward List[Int]; it was the declared type of our
  //  List(1,2) value. But if we declared our List(1, 2) as a List[Any], TypeTag would tell us that we passed a
  //  List[Any] to it."

  // "Ok, here are the two main differences between ClassTags and TypeTags in one place:"
  // - "ClassTag doesn’t know about “higher type”; given a List[T], a ClassTag only knows that the value is a List and
  //    knows nothing about T."
  // - "TypeTag knows about “higher type” and has a much richer type information, but cannot be used for getting type
  //    information about values at runtime. In other words, TypeTag provides runtime information about the type while
  //    ClassTag provides runtime information about the value (more specifically, information that tells us what is the
  //    actual type of the value in question at runtime)."

}
