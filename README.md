# EventScala

## DSL for Expressing Queries

EventScala provides a domain-specific language to express queries. Queries are made up of primitives and operators and can be arbitrarily nested/composed.

Internally, queries expressed using EventScala's DSL are represented by case classes, much like Scala itself. This case class representation can then be passed to EventScala's EventGraph for execution, but it may also be interpreted by any other backend if desired.

### Primitives

+ Subscription to a `stream`

    + Example 1: Subscription to a stream of event instances of type `Event2[Integer, String]`:

    ```scala
    val q: Query =
      stream[Integer, String].from("A")
    ```

### Operators

+ `join` of two streams

    + Example 1: Joining two primitive streams:

    ```scala
    val q1: Query = 
      stream[Integer, String].from("A")
      .join(stream[String, Integer].from("B")).in(slidingWindow(3 seconds), tumblingWindow(3 instances))
    ```

    + Example 2: Joining a primitive and a complex stream:

    ```scala
    val q2: Query =
      stream[java.lang.Boolean].from("C")
      .join(q1).in(slidingWindow(2 instances), tumblingWindow(2 seconds))
    ```

    + Available windows:

    ```scala
    slidingWindow(x instances)
    slidingWindow(x seconds)
    tumblingWindow(x instances)
    tumblingWindow(x seconds)
    ```

+ `select` elements of event instances

     + Example 1: Transforming a stream of event instances of type `Event2[Integer, String]` into a stream of event instances of type `Event1[String]`

    ```scala
    val q1: Query =
      stream[Integer, String].from("A")
      .select(elements(2))
    ```

     + Example 2: Transforming a stream of event instances of type `Event4[Integer, String, String, Integer]` into a stream of event instances of type `Event2[String, String]`

    ```scala
    val q2: Query = 
      stream[Integer, String].from("A")
      .join(stream[String, Integer].from("B")).in(slidingWindow(3 seconds), tumblingWindow(3 instances))
      .select(elements(2, 3))
    ```

+ Defining a filter over elements of event instances

    + Example 1: Only keep those event instances `where` element 1 is smaller than 10.

    ```scala
    val q1: Query =
      stream[Integer, String].from("A")
      .where(element(1) :<: literal(1))
    ```

    + Example 2: Only keep those event instances `where` element 1 equals element 2.

    ```scala
    val q2: Query =
      stream[java.lang.Boolean, java.lang.Boolean].from("D")
      .where(element(1) =:= element(2))
    ```

    + Available comparison operators:
              
    ```scala
    =:= // Equal
    !:= // NotEqual
    :>: // Greater
    >:= // GreaterEqual
    :<: // Smaller
    <:= // SmallerEqual
    ```
              
### Case class representation

Below find a more complex query expressed in EventScala's DSL as well as in its case class representation.

```scala
val subquery: Query =
  stream[String, Integer].from("B")
  .select(elements(2))

val query: Query =
  stream[Integer, String].from("A")
  .join(subquery).in(slidingWindow(3 instances), tumblingWindow(3 seconds))
  .join(stream[java.lang.Boolean].from("C")).in(slidingWindow(1 instances), slidingWindow(1 instances))
  .select(elements(1, 2, 4))
  .where(element(1) <:= literal(15))
  .where(literal(true) =:= element(3))
       
val queryCaseClassRepresentation: Query =
  Filter(
    Filter(
      Select(
        Join(
          Join(
            Stream2("A")[Integer, String],
            LengthSliding(3),
            Select(
              Stream2("B")[String, Integer],
              List(2)),
            TimeTumbling(3)),
          LengthSliding(1),
          Stream1("C")[java.lang.Boolean],
          LengthSliding(1)),
        List(1, 2, 4)),
      SmallerEqual,
      Left(1),
      Right(15)),
    Equal,
    Right(true),
    Left(3))
```


## EventGraph for Executing Queries

TODO
