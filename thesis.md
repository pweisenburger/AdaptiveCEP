# EventScala: A Type-Safe, Distributed and Quality-of-Service-oriented Approach to Event Processing

## Bachelor Thesis of Lucas Bärenfänger

![EventScala](images/logo.png)

### Table of Contents

+ [1 Introduction](#1-introduction)
+ [2 State of the Art](#2-state-of-the-art)
    + [2.1 Overview](#21-overview)
    + [2.2 Language Integration](#22-language-integration)
    + [2.3 Distributed Execution](#23-distributed-execution)
    + [2.4 Quality of Service](#24-quality-of-service)
+ [3 EventScala](#3-eventscala)
    + [3.1 Overview](#31-overview)
    + [3.2 Case Class Representation](#32-case-class-representation)
    + [3.3 Domain-Specific Language](#33-domain-specific-language)
    + [3.4 Execution Graph](#34-execution-graph)
    + [3.5 Quality of Service](#35-quality-of-service)
+ [4 Simulation](#4-simulation)
+ [5 Conclusion](#5-conclusion)
+ [References](#references)

### 1 Introduction

### 2 State of the Art

#### 2.1 Overview

Event processing (EP) has, according to Hinze, Sachs and Buchmann [1], become the "paradigm of choice" in "many monitoring and reactive applications", with application scenarios including traffic monitoring, fraud detection, supply chain management and many more.

This section is structured as follows. Firstly, widely accepted terminology and concepts are described. Secondly, some EP solutions as well as the respective reseach papers are referenced.

Chandy and Schulte [2] simply describe an event as "something that happens". In [1], however, two more refined notions of events are introduced: "change events" (e.g., an object changing its position) and "status events" (e.g., a value yielded by a sensor).

After being observed and signalled, an event takes takes the form of an event instance, which corresponds to an event type. An event instance is commonly represented as a tuple of values, with the type of each element of the tuple being defined in the associated event type. For example, a temperature reading from some sensor "X" indicating 21 degrees celsius in temperature might be represeted by the event instance `("X", 21)` with the event type being `(String, Int)`. For the sake of simplicity, the remainder of this text will refer to event instances as events.

Analoguous to expressions in programming languages, which are either primitive values (e.g., `true`, `42`) or made up of other expressions that are combined by operators and functions (e.g., `true && isThisAGoodNumber(42)`), events may be primitve events or compositions/derivations of primitive and/or other composite/derived events. As defined in [1], "composite events" are "aggregations of events", whereas "derived events" are "caused by other events" and typically are "at a different level of abstraction". As an example for the latter, a series of failed login attemps might cause an intrusion event.

Etzion and Neblett [7] define an "event stream (or stream)" as a "set of associated events" that is "often" "temporally ordered". A stream solely consisting of events of the same type is called a "homogeneous stream"--as opposed to "heterogenous".

Operators are defined over streams as opposed to individual events. The `or` operator, for example, represents the union of two streams, and places the events of both streams in one result stream. The two streams the `or` operator takes as operands can be viewed as its incoming streams, the result stream can be viewed as its outgoing stream.

Traditionally, two approches to EP can be distinguished:

+ Stream processing (SP) typically features operators that resemble those of relational algebra, e.g., `projection`, `selection`, `join`, etc. SP queries are usually expressed in some SQL dialect and constitute so-called "continuous queries". (This term underlines an inversion of principles: In traditional database management systems (DBMSs), it is the data that is being persisted and not the queries. Continuous queries, however, are being persisted and run *continuous*ly, while it is the data that can be thought of as flowing through.)

+ Complex event processing (CEP) typically features operators that resemble those of boolean algebra, e.g., `and`, `or`, `not`. Operators such as `sequence` and `closure` are common, too. CEP queries are usually expressed using rule languages.

Furthermore, there is another significant difference between SP and CEP. As said, SP operators resemble those of relational algebra. Some relational operators (e.g., `join`), however, are blocking operators in the sense that they are defined over finite sets of data and block execution until these are available in their entirety. Streams, however, can be viewed as infinite sets of data. As a consequence, in SP, the respective operators are not applied to streams directly. Instead, they require their operand streams to be annotated with so-called windows, which are typically expressed "in terms of time or number of tuples [i.e., events]" [30]. A window only contains a finite number of events of the respective stream. So-called consumption modes are the counterpart of windows in CEP. They are explained best through an example. The CEP operator `and`, for instance, is semantically ambiguous. The query `A and B` only specifies that an event of type `A` should be correlated with an event of type `B`. However, given the events `b1`, `b2`, `a1`, occuring in that order, it is not clear whether `a1` should be correlated with `b1` or `b2`--this depends on the given consumption mode. (See [19] for more information on consumption modes.) One of the differences between windows and consumptions modes is that the former are applied to streams (i.e., operands) while the latter are applied to operators. As pointed out in [30], consumption modes "can be loosely interpreted as load shedding, used from a semantics viewpoint rather than a QoS viewpoint" as they essentially dictate which events have to be kept in memory and which ones can be dropped as they will not be part of any correlation. To the best of my understanding, the same can be said about windows.

Many EP solutions do feature operators of both approaches. However, with regards to the SP solutions I have looked at, CEP operators are usually treated as second-class citizens. Esper [3], for example, can be considered a SP engine, coming with a typical SQL dialect, EPL (Event Processing Language), for expressing queries. Queries solely made up of CEP operators can be expressed using so-called `pattern`s. These can then be used as operands of SP operators. (Listing 1.a) It is not possible to use SP operators within a pattern, though. (Listing 1.b) Another solution, Flink [4], which considers itself to be a "stream processing framework", also features CEP operators (e.g., sequence as `followedBy`)--but does so in a designated library, called FlinkCEP [5].

*Listing 1.a: In EPL, the `join` operator (`,`) can be applied to a `pattern`. (`lastEvent` forms a window, continuously containing the last event of the stream.)*
```sql
select * from Sensor1.std:lastEvent(), pattern[every (Sensor2 or Sensor3)].std:lastEvent()
```

*Listing 1.b: On the contrary, in EPL, the `join` operator cannot be used within a `pattern`.*
```sql
// Invalid EPL!
select * from pattern[every (Sensor1 or (Sensor2.std:lastEvent(), Sensor3.std:lastEvent()))]
```

It is to be pointed out that the distinction between SP and CEP can be considered blurry, as many books and publications often use the terms SP and CEP in their borader sense, that is, EP in general. SP and CEP do, however, pose different challenges when it comes to quality of service (QoS). (See section 2.4. regarding their QoS-related differences.) Refer to section 5 ("Analysis of Event vs. Stream Processing") of [6] for another comparison of CEP an SP.

Moreover, the term event-driven architecture (EDA) represents another important concept in the domain of EP. It is defined as the "concept of being event-driven", i.e., acting in response to an event, being applied to software. In [2], the following "five principles of EDA" are identified:

+ Individuality: Each event is transmitted individually, not as part of a batch.
+ Push: Events are pushed, not pulled, i.e., requested.
+ Immediacy: Events are being reacted to immediately after reception.
+ One-way: The type of communication is "fire-and-forget", events are neither being acknowledged nor replied to.
+ Free of command: An event never prescribes the action that will be taken upon reception.

Early EP solutions are HiPAC [17], SAMOS [18], Snoop [19] and SnoopIB [20]. To the best of my knowledge, these will give an impression of the developments from the late 1980s to the early 2000s. Up-to-date solutions are, for example, Esper [3] and Flink [4].

#### 2.2 Language Integration

This section starts out by examining and criticizing the way queries are typically expressed in EP solutions. Then, the notion of domain-specific languages (DSLs) is introduced. Lastly, it is described how DSLs are used to ease problems common in the context of relational databases that are similar to the previously described problems present in the EP domain.

As pointed out in [8], "there does not exist any generally accepted definition language for complex event processing". However, many EP solutions--especially those that rely on dialects of SQL for expressing queries--generally receive those in the form of strings, just as traditionally done in DBMSs.

In [22], Leijen and Meijer lay out why communicating SQL expressions in the form of "unstructured strings" is a bad approach. Firstly, "[p]rogrammers get no static safeguards against creating syntactically incorrect or ill-typed queries, which can lead to hard to find runtime errors". Furthermore, it is noted that the programmer--at least--needs to know the SQL dialect as well as the "language that generates the queries and submits them". To underline these very points, the authors of [24] present "a very simple example of an SQL query in Java" that contains multiple errors. The SQL query is embedded in Java as a `String`. Among other errors, it contains a misspelled SQL command. Furthermore, it is stated that the assumtions made about the type of the column and the result set could be wrong. As said, the problem is that, as described by Spiewak and Zhao [23], the query is embedded "within application code in the form of raw character strings". As "[t]hese queries are unparsed and completely unchecked until runtime", malformed queries do not cause the code they are embedded in to not compile but will cause trouble when being executed run-time.

To tackle this issue in the domain of traditional DBMSs, the contribution of [23] is a so-called domain-specific language (DSL)--named ScalaQL--"to make the host language compiler [i.e., scalac] aware of the query and capable of statically eliminating these runtime issues". (In fact, the contribution of the much earlier released publication of Leijen and Meijer [22] is very similar. Here, the host language is Haskell, the problem domain is also relational databases, though. They appear to be the first ones to "show how to embed the terms and type system of another (domain-specific) programming language into the Haskell framework, which dynamically computes and executes programs written in the embedded language".

To explain what DSLs are, it makes sense to turn to the book "DSLs in Action" by Ghosh [21], which appears to be the standard primer on the subject. In it, one can find the following definition:

> "A DSL is a programming language that's targeted at a specific problem; other programming languages that you use are more general purpose. It contains the syntax and semantics that model concepts at the same level of abstraction that the problem domain offers."

Furthermore, it is stated that--"[m]ore often than not"--DSLs are used by non-expert programmers. For this to be possible, it is necessary that the DSL appeals to its target group by having "the appropriate level of abstraction", that is, it must embody the particular terminology, idioms, etc. of the respective domain. This, however, leads to so-called "limited expressivity"--"you can use a DSL to solve the problem of that particular domain only". Exemplary, the author mentions that "[m]athematicians can easily learn and work with Mathematica" and "UI designers feel comfortable writing HTML". Regarding limited expressivity, however, it is noted that one cannot, for example, "build cargo management systems using only HTML".

In his book, Ghosh further differentiates the notion of DSLs. According to the author, there are internal as well as external DSLs. (Internal DSLs are also known as embdedded DSLs.) They are defined as follows:

> "An internal DSL is one that uses the infrastructure of an existing programming language (also called the host language) to build domain-specific semantics on top of it."

> "An external DSL is one that's developed ground-up and has separate infrastructure for lexical analysis, parsing technologies, interpretation, compilation, and code generation."

In [22], it is argued that internal DSLs "expressed in higher order, typed [...] languages" are better suited as a "framework for domain-specific abstractions" than external DSLs. This is, according to the authors, due to the fact that programmers only have to know the host language as domain specific abstractions are made available as "extension languages". Other advantages mentioned are the possibility to leverage other "domain-sprecific libraries" as well as the possibility to use the host language's infrastructure, e.g., its module and type system. In [24], more advantages of internal DSLs are stated, e.g., "re-using the platform tooling, which in Java case [sic] includes compilers, advanced IDEs, debuggers, profilers, and so on". Furthermore, it is stated that development is much easier as it essentially "boils down to writing an API".

Finally, it is to be noted that Scala lends it self very well as a host language for internal DSLs. For instance, Scala language features used in ScalaQL [4] include "operator overloading, implicit conversions, and controlled call-by-name semantics". With ScalaQL deprecating SQL strings as a means of communication with DBMSs in the Scala ecosystem, it is the logical next step to deprecate SQL-like strings as a means of communication with EP solutions in the Scala ecosystem--by developing an internal DSL. (Obviously, ScalaQL is just one of many DSLs doing so, another approach among many others would be Slick by Lightbend (formerly Typesafe) [31].)

#### 2.3 Distributed Execution

In [8], Schilling, Koldehofe, Rothermel and Pletat argue that "distributing the handling of events" is of growing importance, if only due to "the emerging increase in event sources". It can be stated that other reasons for this trend are the need to exploit parallelism to meet increasing efficiency demands, avoiding single points of failure, and many more.

Academia has, in fact, proposed numerous approaches to distributed event processing, e.g, [8 - 16]. To the best of my knowledge, however, these have too few common characteristics to distill one reference model that represents academia's approach to distributed EP. Furthermore, as pointed out in [8], "there exists a gap between [...] academia and the industry", that is, none of the approaches to distributed EP proposed have actually been picked up and used in practice. One of the stated reasons for this circumstance is that EP technology "in business applications" needs "to access context information related to business processes" that "often resides in centralized databases".

As a consequence of academia's propositions being too diverse to be adequately represented in this section as well as because of the lack of solutions originating in industry, I chose to introduce distributed EP theoretically, i.e., through an abstraction. Afterwards, challenges that are present in this area of research are listed--along with references to publications that address these.

In [7], Etzion and Niblett introduce an abstaction called event processing network (EPN). An EPN is made up of processing elements. It is represented as a graph with the processing elements being the nodes of the graph. Nodes can have "input terminals" and/or "output terminals". Output terminals of one node may be connected to input terminals of other nodes, thus forming the edges of the graph. An edge shows "the flow of event instances through the network", thus can be considered a stream. There are the following types of processing elements:

+ Event producers introduce events into an EPN.
+ Event consumers receive events from processing elements in an EPN.
+ Event processing agents (EPAs) embody "three logical functions":
    + Filtering, i.e., selecting events for further processing
    + Matching, i.e., "[f]inding patterns among events" and creating respective pattern events
    + Derivation, i.e., deriving new events from the output of the pattern step
+ Global state elements "represent stateful data that can be read by event processing agents when they do their work".
+ Channels may be used to connect processing elements instead of edges, as the behavior of channels may be defined explicitly.

```
todo
add illustration
```

It is to be stressed that an EPN diagram is an abstraction, comprised of "platform-independend definition elements". It does not assume a distributed implementation. After the introduction of the EPN concept, the authors lay out the "[i]mplementation perspective", in which the graph has to be materialized onto what are being called "runtime artifacts", resulting in a "runtime system". Usually, there is no "one-to-one correspondence" between the processing elements of an EPN and the runtime artifacts of a respective implementation. With regards to this, two "extremes" are being described:

+ The entire EPN may be represeted by one runtime artifact, i.e., one centralized runtime system.

+ Each EPA is represented by one runtime artifact. These distribute events between each other and "can be placed on different server[s], allowing much of the [...] work to be performed in parallel."

To the best of my knowledge, the second scenario can be considered a good example for distributed EP, even though there are obviously many ways to map EPAs to runtime artifacts, several of which might also be considered distributed approaches.

In the chapter "EPA assignment optimization"--which is concerned with mapping "logical functions [i.e., EPAs] to physical runtime artifacts"--the authors mention parallel and distributed processing explicitly. Parallel processing is considered "[o]ne of the major ways to achieve [...] performance metrics". Then, three levels of parallelism are introduced, that is, using multiple threads in one core, using multiple cores and using multiple machines. However, finding out "which activities should be run in parallel" is listed as a "difficult" challenge. Regarding distributed processing, it is stated that "moving the processing close to the producers and consumers" constitutes an optimization method. Furthermore, "[p]artitioning"--the practice of grouping EPAs so they eventually "execute together [i.e., in parallel]" when mapped to runtime artifacts--is introduced and considered "key" to both parallel execution and distributed execution. "Stratification", a partitioning approach in which EPAs are assigned to so-called "strata" is then explained: "If `EPA1` produces events that are consumed by `EPA2`, then `EPA2` is placed in a higher stratum."

```
todo
add illustration
```

Regarding the aforementioned challenge of parallelization, [12 - 15] present different approaches. Furthermore, the previously mentioned "moving [of] the processing close to the producers and consumers" poses another challenge, especially with mobile producers/consumers. In [10, 11, 16], interesting approaches with regards to this are presented. Moreover, it appears that a good amount of the solutions to distributed EP proposed by academia, e.g., [9, 10, 11, 13], are built upon loosely-coupled publish/subscribe systems (or pub/sub systems), which constitue a related field of research themselves. Also, for events to be put into time-based windows (as sometimes required by the SP operator `join`) or for them to be ordered propery (as demanded by the CEP operator `sequence`), they need to be properly timestamped. Timestamping is a well-studied challenge in distributed systems in general and many approaches to tackle it have been proposed. [9] presents an interesting solution specifically aimed at distributed EP, leveraging a combination of NTP-synchronized local clocks and heartbeat events.

#### 2.4 Quality of Service

In [26], Appel, Sachs and Buchmann note that "[f]uture software systems" have to be "responsive to events" in order to "adapt [...] software to enhance business processes". As an example, they mention logistics processes that must be altered according to incoming updates on traffic. Furthermore, it is stressed that in such a scenario, in which critical business processes are triggered by events, "[t]he trust in such [...] systems depends to a large extent on the Quality of Service (QoS) provided by the underlying event system". The definition of QoS metrics as well as finding the means to monitor those is considered a "major challenge".

This section is structured as follows. Firstly, QoS metrics that have been identified for publish/subscribe systems--which also apply to EP in general--are described. Secondly, QoS metrics that are specific to SP or CEP are presented.

As--according to [26]--"[f]or the [underlying] communication [...] the paradigm of choice is publish/subscribe" [26], it makes sense to take a look at [27], in which Behnel, Fiege and Mühl identify generic QoS metrics for publish/subscribe-based systems. It is to be noted, however, that the authors of [26] recommended to refrain from considering such generic QoS metrics. Instead, they propose to first identify "functionality needs" of an EP solution and then, based on this, compile a list of specific QoS requirements--"rather than building a Swiss army knife EBS [i.e., event-based system] supporting all imaginable [...] QoS needs". Nevertheless, the following paragraphs summarize some of the generic QoS metrics presented in [27]. Before that, a short description of the publish/subscribe paradigm is quoted, also from [27]:

> "The system model of the publish-subscribe communication paradigm is surprisingly simple. [... There are] three roles: publishers, subscribers and brokers. Publishers [...] provide information, advertise it and publish notifications about it. Subscribers [...] specify their interest and receive relevant information when it appears. Brokers mediate between the two by selecting the right subscribers for each published notification. [...] There can be a single centralized broker, a cluster of them or a distributed network of brokers."

Although the QoS metrics listed below were originally put together "in the context of distributed and decentralized publish-subscribe systems", they do also apply to EP, since--as mentioned before--the underlying communication of many distributed EP solutions is based on the publish/subscribe paradigm. In order to interpret these QoS metrics in the sense of EP, one can simply think of event producers, event consumers and EPAs (as found in EPNs) whenever publishers, subscribers and brokers (as found in publish/subscribe systems) are mentioned. Moreover, statements about publishers and subscribers do not only apply to event producers and event consumers, respectively, but also to EPAs, as they also cosume and produce event streams. For example, a statement such as "Publishers annotate notifications they emit with priorities" can be interpreted as "Event producers and EPAs annotate events they produce with priorities".

+ Latency: It is stated that "[s]ubscribers request a publisher that is within a maximum latency bound". An "end-to-end latency" between a publisher and a subscriber "depends on the number of [...] hops between them" as well as on the time each broker takes to deal with a notification. However, in a distributed setting, "measured lower bounds" may at best "give hints" about whether some latency requirement can be met--not "absolute guarantees". Preallocating paths is mentioned as a way to deal with latency requirements.

+ Bandwith: It is described that publishers announce upper/lower bounds regarding the stream they produce, whereas subscribers "restrict the maximum stream of notifications they want to receive". It is recommended to consider bandwidth at the "per-broker" level. Given that "each broker knows the bandwidth it can make locally available to the infrastructure", an upper-bound for an entire path can be estimated, allowing for routing "based on the highest free bandwidth".

+ Message priorities: It is proposed that publishers annotate the notations they produce with relative or absolute priorities, denoting their importance compared to other notifications produced either by themselves or elsewhere, respectively. Subscribers, on the other hand, specify priorities regarding their subscriptions. At the "per-broker" level, priorities "can be used to control the local queues of each broker", resulting in the "end-to-end application" of the priorities along the entire path. This is, according to the authors, commonly implemented by letting notifications with higher priorities overtake those with lower priorities at each broker.

+ Delivery guarantees: While subscribers announce "which notifications they must receive", and where they are sensitive to duplicates, it is stated that publishers "specify if subscribers must receive certain notifications". A simple approach that is mentioned is to simply let the system know which messages can be dicarded. More sophisticated approaches concern "the completeness and duplication of delivery", i.e., subscribers may receive notifications that are directed to them "at least once", "at most once", or "exactly once". While meshing is listed as a way to achieve "at least once", it comes at the price of "increasing the message overhead". Disconnected subscribers are yet "[a]nother problem", as they might stay disconnected, in which case "at least once" cannot be guaranteed, no matter how long the notifications are buffered.

+ Notification order: It is explained that "[t]he order in which notifications arrive" is of significance in some cases while it is not in other cases. With centralized ordering, "ordering is [considered] easy to achieve". However, "distributed ordering of events coming from different sources" is described as problematic. Deploying a "central broker to enforce a global odering" is mentioned as a "generic approach" to tackle this challenge, imposing a "limit" on the "scalability of the overall infrastrucure", though.

+ Validity interval: The importance of the infrastructure to know "how long a notification stays valid" is stressed. It is explained that this is specified either "in terms of time" of "by the arrival of later messages". Specifying that "only the most recent event is of interest" through "follow-up messages" is described as an example of the latter. This is called an "efficient approach" as it allows for the infrastructure to "shorten its queues in high traffic situations".

When interpreting the above list of QoS metrics in terms of EP, it becomes evident that these metrics can be applied to both SP as well as CEP. (As said, brokers are to be thought of as EPAs, and it has not been specfied whether these EPAs perform SP or CEP operations.) There are, however, QoS metrics that specifically apply to either SP or CEP. Some have been identified in [28] by Buchmann, Appel, Freudenreich, Frischbier and Guerrero.

SP-specific QoS metrics listed in the section "QoS of Stream Processing" include:

+ Timeliness: One the one hand, it is stated that applications relying on SP "typically have timing contraints to meet", thus, the timeliness of the "continuous processing of incoming events" is considered "one of the most relevant QoS requirements". On the other hand, however, many of these applications may "tolerate approximate results". According to the authors, these circumstances suggest a "common" "trade-off", i.e., "accuracy for timeliness". To the best of my understanding, an example of this trade-off would be favoring some less precise filter that therefore executes quickly over some more precise and more time-consuming filter.

+ Throughput: With regards to timeliness, "achievable throughput" is considered a "closely related QoS metric". SP solutions are said to "attempt to optimize" their "continuous query execution" in order to achieve maximum thoughput. As load shedding is regarded as "often" being the "only practical approach", the result is, again, the "trade-off of accuracy for timeliness". (The authors stress that the expected timeliness as well as the expected accuracy must be specified when designing a business process that relies on the SP.)

+ Order: Whether events may be processed out of order is listed as an "application dependent" "issue".

CEP-specific QoS metrics listed in the section "QoS of Event Composition" include:

+ Order: Establishing an "odering between events" can be considered the most important QoS metric, as "achievable QoS [...] depends lagely on the possibility" to be able to do so. As an example of an operator requiring events to be ordered, the `sequence` operator--"which is part of most [CEP] event algebras"--is mentioned. Furthermore, it is stated that the "natural ordering" is time-based, which, according to the authors, is no problem if "there is only one central clock" as well as only one event occuring every clock tick. If, however, there may be "multiple events" occuring at the same point in time, being "time-stamped by different clocks", establishing a total order is said to be impossible. Lastly, it is explained that the granularity of timestamps plays a major role when it comes to timestamping: Events that might have been distinguishable with fine-grained timestamps might no be distinguishable when being timestamped more coarsely.

+ Delay/loss of messages: The delay or loss of messages is described as a "source of ambiguity". As an example, it is explained that it is impossible to determine that an event "did not occur in a given interval" unless it can be asserted that the event in question is neither delayed nor lost. With regard to bounded networks, the authors refer to the 2g-precedence model as a possibility to tackle this challenge. (An explanation of this model can be found in [9].) With regard to unbounded networks--"such as the internet"--they refer to [9], i.e., an approach based on the the injection of "heartbeat events from an outside time-service" which assumes that events "in the same channel do not overtake each other".

### 3 EventScala

#### 3.1 Overview

#### 3.2 Case Class Representation

At the heart of the EventScala framework is the case class representation of queries. Thanks to it, EventScala's DSL and EventScala's execution graph can be thought of as separate modules that do not depend on each other in any way. On the one hand, using the DSL to express a query results in the case class representation of that query. On the other hand, executing a query using the execution graph requires a case class representation of said query. However, a case class representation of a query does neither have to be obtained using the DSL nor does the query it represents have to be executed using the execution graph. In fact, one could, for example, use the DSL to obtain the case class representation of a query and then generate a SQL-like string from it to execute it using Esper. Likewise, one could develop a different DSL or even type out the case class representation of a query by hand and then pass it to the execution graph to be run. Essentially, EventScala's case class representation is a type-safe and platform-independend way to encode queries for EP systems in Scala.

In this section, EventScala's case class representation is presented in great detail. To this end, the hierarchy and structure of the traits and case casses that make up the case class representation of queries is explained.

At the top of the hierarchy, there is the `Query` trait, which only specifies that extending classes have to have a field `requirements`, representing a set of QoS requirements that were specified for the respective query. (Refer to the end of this section as well as section 3.5 for more information on QoS requirements.)

As described in section 2.1, events are commonly represented by tuples of values, i.e., an event consisting of n values would be represented by a n-tuple. In EventScala, an event consists of at least 1 element and at most 6 elements. Analoguously, the `Query` trait is extended by the traits `Query1`, `Query2`, ..., `Query6`, each representing a query that results in a stream of events consisting of 1, 2, ..., 6 elements, respectively. The traits `Query1`, `Query2`, ..., `Query6` do not specify any additional fields. They do, however, take 1, 2, ..., 6 type parameters, respectively, specifying the types of the elements of the events of the respective streams they represent. `Query2[String, Int]` is, for example, the type of a query that results in a stream of events consisting of two elements: a `String` and an `Int`.

A query usually consists of nested applications of operators over primitives. For example, one might subscribe to a stream of events consisting of a single `Int`, resulting in a `Query1[Int]`, as well as to a stream of events consisting of two `String`s, resulting in a `Query2[String, String]`. One might then apply the `join` operator to these two stream subscriptions, resulting in a `Query3[Int, String, String]`. Afterwards, one could decide to drop the second `String` using the `dropElem3` operator, resulting in a `Query2[Int, String]`. When picturing this query as a graph, the `dropElem3` operator would be the root node, having one child node, i.e., the `join` operator, which, in turn, would have two child nodes, i.e., the two stream subscriptions, which would represent the leaves of the graph. (This is actually exactly what EventScala's execution graph for this query would look like.) Analoguously, the application of the `dropElem3` operator can be thought of as a unary query--unary in the sense that it has one subquery. Furthermore, the application of the `join` operator can be thought of as a binary query--binary in the sense that it has two subqueries. Lastly, the subscription to a stream can be thought of as a leaf query--leaf in the sense that it has no child queries. As a matter of fact, in EventScala, every query can be classified into being either a leaf, a unary or a binary query. Therefore, there exist three traits `LeafQuery`, `UnaryQuery` (specifiying one field, `sq` (`s`ub`q`uery), of type `Query`) and `BinaryQuery` (specifiying two fields, `sq1` and `sq2`, both of type `Query`). All three traits extend the trait `Query`.

```
todo
add illustration
```

In the previous paragraph, it has been hinted that in EventScala, one might subscribe to streams or make use of the operators `join` as well as `dropElem3`. For the sake of completeness, find below the list of all primitives (i.e., leaf queries) and operators (i.e., unary and binary queries) availiable.

Leaf queries, i.e., traits extending `LeafQuery`:

+ `StreamQuery`: A `StreamQuery` expresses a subscription to a stream. The trait `StreamQuery` specifies one field of type `String`, `publisherName`, for the name of the publisher that is the source of the stream.

+ `SequenceQuery`: A `SequenceQuery` expresses a subscription to two streams with the CEP operator `sequence` being applied to them. The trait `SequenceQuery` specifies two fields of type `NStream`, `s1` and `s2`. A `NStream` is essentially a `Stream`, however, it is "`N`ot a query", i.e., not extending the trait `Query`. If `NStream` would be a query, then `s1` and `s2` would represent its subqueries, making `Sequence` a binary query rather than a leaf query.

Unary queries, i.e., traits extending `UnaryQuery`:

+ `FilterQuery`: A `FilterQuery` expresses the application of the SP operator `where`. The `FilterQuery` trait specifies one field of type `Event => Boolean`, `cond`, for the filter predicate. The field for the subquery representing the operator's input stream is specified by the extending classes.

+ `DropElemQuery`: A `DropElemQuery` expresses the application of the operator that EventScala offers instead of the EP operator `select`. While `select` lets one select which elements of the events of the operator's input stream to keep, the `dropElem` operator lets one specify which element to drop. The `DropElemQuery` trait specifies no fields. Which element of the events of the operator's input stream is to be dropped is specified by the extending classes' names, e.g., `DropElem1Of2`.

+ `SelfJoinQuery`: A `SelfJoinQuery` expresses the application of the SP operator `join` but with one stream representing both of the operator's input streams. The `SelfJoinQuery` trait specifies two fields of type `Window`, `w1` and `w2`, for the windows that are applied to the operator's input streams. The field for the subquery representing both of the operators's input streams is specified by the extending classes.

Binary queries, i.e., traits extending `BinaryQuery`:

+ A `JoinQuery` expresses the application of the SP operator `join`. The `JoinQuery` trait specifies two fields of type `Window`, `w1` and `w2`, for the windows that are applied to the operator's input streams. The fields for the two subqueries representing the operators's input streams are specified by the extending classes.

+ A `ConjunctionQuery` expresses the application of the CEP operator `and`. The `ConjunctionQuery` trait specifies no fields. The fields for the two subqueries representing the operator's input streams are specified by the extending classes.
+ A `DisjunctionQuery` expresses the application of the CEP operator `or`. The `DisjunctionQuery` trait specifies no fields. The fields for the two subqueries representing the operator's input streams are specified by the extending classes.

Up to this point, a hierarchy of traits but not one case class has been presented. The case class representation of a query is, however, made up of (possibly nested) case classes. These case classes have the following in common:

  + They extend exactly one of the traits `Query1`, `Query2`, ..., `Query6`, indicating the number and types of the elements of the events of the resulting stream.

  + They extend exactly one of the traits `StreamQuery`, `SequenceQuery`, `FilterQuery`, `DropElemQuery`, `SelfJoinQuery`, `JoinQuery`, `ConjunctionQuery` and `DisjunctionQuery`, indicating what kind of primitive or operator application they represent.

For example, the case class `Stream1[A]` extends the trait `Query1[A]`, indicating that it represents a stream of events consisting of `1` element of the generic type `A`, as well as the trait `StreamQuery`, indicating that it respresents a subscription to a stream, i.e., a primitive. It has a field `publisherName` as specified by the trait `StreamQuery` as well as a field `requirements` as specified by the trait `Query`. `Conjunction12[A, B, C]`, to provide another example, extends the trait `Query3[A, B, C]`, indicating that it represents a stream of events consisting of `3` elements of the generic types `A`, `B` and `C`, respectively, as well as the trait `ConjunctionQuery`, indicating that it represents an application of the `and` operator. Obviously, it also has the field `requirements`.

More interestingly, though, the case class `Conjunction12[A, B, C]` also has two fields (`sq1` of type `Query1[A]` and `sq2` of type `Query2[B, C]`) for the subqueries representing the `and` operator's two input streams, which--against one's intuition--are not specified in the `ConjunctionQuery` trait. The reason for this is that while every case class extending `ConjunctionQuery` does have two fields named `sq1` and `sq2`, their types are always different. For the case class `Conjunction11[A, B]`, their types are `Query1[A]` and `Query1[B]`, respectively, and for `Conjunction21[A, B, C]`, to provide another example, they are `Query2[A, B]` and `Query1[C]`, respectively. This hints at the biggest shortcoming of EventScala: As it is--to the best of my knowledge--not possible to abstract over the length of type parameter lists in Scala, thus, one cannot define one trait (like `trait Query[A, B, ...]`) for queries in general, but has to define one separate trait (e.g., `Query1[A]`, `Query2[A, B]`, and so forth) for queries representing streams of events consisting of 1 element, 2 elements, and so forth. (Tuples actually suffer from the same shortcoming: `Tuple1[+T1]`, `Tuple2[+T1, +T2]`, and so forth, are all defined separately in the Scala standard library.) To avoid a ridiculous amount of code repetition, I decided that in EventScala, events can consist of at most 6 elements. This seemingly small number does not only call for the 6 separate traits `Query1[A]` to `Query6[A, B, C, D, E, F]`, though, but also for--for example--15 separate case classes extending the `SequenceQuery` trait as well as, to provide the most extreme example, for 36 case classes extending the `DisjunctionQuery` trait. Any future work on this project should aim to solve this issue, maybe by representing events and queries with `HList`s, which encode the types of their members (which can be `H`eterogenuous, i.e., of different types) in their type. They are provided by Typelevel's Shapeless [32], a "type class and dependent type based generic programming library for Scala".

At this point, EventScala's case class representation has been explained to an extent that it makes sense to present an example query in case class representation (listing 2):

*Listing 2: Case class representation of a query*
```scala
val sampleQuery1: Query2[Int, String] =
  DropElem3Of3[Int, String, String](
    Join12[Int, String, String](
      Stream1[Int]("X", Set.empty),
      Stream2[String, String]("Y", Set.empty),
      SlidingTime(42),       // Sliding window of 42 seconds
      TumblingInstances(21), // Tumbling window of 21 events
      Set.empty),
    Set.empty)
```

The avid reader has certainly noticed that this is the case class representation of the query that has been informally described and illustrated as a graph previously in this section. It is to be stressed that this is a type-safe representation of said query. (This point will be discussed in detail in section 3.3.) Also, it is to be stressed again that it is a platform-independent representation, i.e., it neither encodes data specific to the DSL that generated it nor does it encode data that is specific to the EP solution that will execute it.

Lastly, as the title of this thesis suggests, EventScala is a quality-of-service-oriented approach to EP. As such, it allows for QoS requirements to be expressed with each query. One might have noticed that the `sampleQuery` above contains the expression `Set.empty` four times. At these four points, it would have been possible to define a set of requirements (of type `Set[Requirement]`). When picturing the query as a graph once again, it becomes clear that requirements can be defined over every node of the graph. EventScala features two kinds of requirements, `FrequencyRequirement`s and `LatencyRequirement`s. (These will be discussed in greater detail in section 3.5.) It is to be noted, however, that the case classes representing these requirements are not platform-independent. They do not just contain the specification of the respective requirement but also a callback closure that defines what to do whenever the respective requirement is not met during the execution of the query. The fact that this callback closure is being passed data about the processing node that is responsible for executing the query in EventScala's execution graph is what breaks platform independence. I chose to go this way as platform independece with regards to QoS requirements is somewhat pointless as --to the best of my knowledge--there are no other EP solutions that are able to work with such requirements. Listing 3 shows another example query, representing a simple stream subscription that is being applied to a filter, with the stream being required to emit at least 2 events every 5 seconds.

*Listing 3: Case class representation of a query that specifies a QoS requirement*
```scala
val sampleQuery2: Query2[Int, Boolean] =
  Filter2[Int, Boolean](
    Stream2[Int, Boolean](
      "Z",
      // If not at least 2 events are emitted every 5 seconds, "Problem!" will be printed to the console.
      Set(FrequencyRequirement(GreaterEqual, 2, 5, _ => println("Problem!")))),
    // This filter predicate does not filter out any event. :)
    _ => true,
    // No QoS requirements are defined over the filter.
    Set.empty)
```

#### 3.3 Domain-Specific Language

EventScala's DSL for expressing queries for EP systems sets out to achieve in the domain of EP what ScalaQL and other DSLs achieved for relational databases, i.e., "statically eliminating [...] runtime issues" [23] caused by "syntactically incorrect or ill-typed queries" [22]. EventScala's DSL can be classified as an internal DSL. With EventScala being a Scala framework, its host language is obviously Scala.

```
todo
mention typesafety, ide support
```

This section is structured as followed. Firstly, the DSL is presented from a user's perspective, i.e., an overview of  its features is given and advantages are pointed out. Then, notable parts of its implementation are discussed, e.g., the use of Scala features such as implicit conversion [33].

The section presenting the case class representation of queries already revealed which primitives and operations are supported by EventScala. In the following listings (`TODO reference`), it will be shown how the DSL can be used to express queries made up of these primitves and operators.

*Listing 4: Primitives, i.e., leaf queries*
```scala
// Subscription to a stream
// of events of type `Int` from A
val stream1: Query1[Int] =
  // Here, the type has to be explicitly annotated:
  stream[Int]("A")

// Subscription to a stream
// of events of type `String, String` from B
val stream2: Query2[String, String] =
  stream[String, String]("B")

// Subscription to two streams
// of events of type `Int` and `Boolean`, respectively,
// with the CEP operator `sequence` applied to them
val sequence1: Query2[Int, Boolean] =
  // Here, the types have to be explicitly annotated:
  sequence(
    nStream[Int]("C") ->
    nStream[Boolean]("D"))

// Subscription to two streams
// of events of type `Int, Int` and `Int`, respectively,
// with the CEP operator `sequence` applied to them
val sequence2: Query3[Int, Int, Int] =
  sequence(
    nStream[Int, Int]("E") ->
    nStream[Int]("F"))
```

*Listing 5: Application of unary operators, i.e., unary queries*
```scala
// Application of the SP operator `where`
// to `stream1` (`Query1[Int]`)
val filter1: Query1[Int] =
  stream1.where(_ % 2 == 0)
  // Alternatively: `stream1 where (_ % 2 == 0)`

// Application of the SP operator `where`
// to `sequence2` (`Query3[Int, Int, Int]`)
val filter2: Query3[Int, Int, Int] =
  sequence2.where((e1, _, e3) => e1 < e3)

// Application of the SP operator `dropElem2`
// to `stream2` (`Query2[String, String]`)
val dropElem1: Query1[String] =
  stream2.dropElem2()
  // Alternatively: stream2 dropElem2()

// Application of the SP operator `dropElem1`
// to `sequence1` (`Query2[Int, Boolean]`)
val dropElem2: Query1[Boolean] =
  sequence1.dropElem1()

// Application of the SP operator `selfJoin`
// to `stream2` (`Query2[String, String]`)
val selfJoin1: Query4[String, String, String, String] =
  stream2.selfJoin(
    slidingWindow(42.instances), // Alternatively: `42 instances`
    tumblingWindow(42.seconds))  // Alternatively: `42 seconds`

// Application of the SP operator `selfJoin`
// to `sequence1` (`Query2[Int, Boolean]`)
val selfJoin2: Query4[Int, Boolean, Int, Boolean] =
  sequence1.selfJoin(
    tumblingWindow(42.instances),
    slidingWindow(42.seconds))
```

*Listing 6: Application of binary operators, i.e. binary queries*
```scala
// Application of the SP operator `join`
// to `stream1` (`Query1[Int]`)
// and `stream2` (`Query2[String, String]`)
val join1: Query3[Int, String, String] =
  stream1.join(
    stream2,
    slidingWindow(42.instances),
    tumblingWindow(42.seconds))

// Application of the SP operator `join`
// to `sequence1` (`Query2[Int, Boolean]`)
// and `stream2` (`Query2[String, String]`)
val join2: Query5[Int, Boolean, Int, Int, Int] =
  sequence1.join(
    sequence2,
    slidingWindow(42.instances),
    tumblingWindow(42.seconds))

// Application of the CEP operator `and`
// to `stream2` (`Query2[String, String]`)
// and `stream1` (`Query1[Int]`)
val conjunction1: Query3[String, String, Int] =
  stream2.and(stream1)
  // Alternatively: `stream2 and stream1`

// Application of the CEP operator `and`
// to `sequence1` (`Query2[Int, Boolean]`)
// and `sequence2` (`Query3[Int, Int, Int]`)
val conjunction2: Query5[Int, Boolean, Int, Int, Int] =
  sequence1.and(sequence2)
  // Alternatively: `sequence1 and sequence2`

// Application of the CEP operator `or`
// to `stream1` (`Query1[Int]`)
// and `stream2` (`Query2[String, String]`)
val disjunction1: Query2[Either[Int, String], Either[X, String]] =
  stream1.or(stream2)
  // Alternatively: `stream1 or stream2`

// Application of the CEP operator `or`
// to `sequence2` (`Query3[Int, Int, Int]`)
// and `sequence1` (`Query2[Int, Boolean]`)
val disjunction2: Query3[Either[Int, Int], Either[Int, Boolean], Either[Int, X]] =
  sequence2.or(sequence1)
  // Alternatively: `sequence1 or sequence2`
```

*Listing 7: Nested queries*
```scala
// Nested application
// of 3 unary and 3 binary operators
// to 4 primitives
val nested1: Query3[Either[Int, String], Either[Int, X], Either[Float, X]] =
  stream[Int]("A")
    .join(
      stream[Int]("B"),
      slidingWindow(2.seconds),
      slidingWindow(2.seconds))
    .where(_ < _)
    .dropElem1()
    .selfJoin(
      tumblingWindow(1.instances),
      tumblingWindow(1.instances))
    .and(stream[Float]("C"))
    .or(stream[String]("D"))

// Nested application
// of 2 binary operators
// to 3 (!) primitives
val nested2: Query4[Int, Int, Float, String] =
  stream[Int]("A")
    .and(stream[Int]("B"))
    .join(
      sequence(
        nStream[Float]("C") ->
        nStream[String]("D")),
      slidingWindow(3.seconds),
      slidingWindow(3.seconds))
```

When examining queries that are expressed using the DSL (such as the ones listed above), many advantages over expressing queries as unstructured strings become apparent, including the following:

+ Syntax: Obviously but nevertheless very important is the fact that syntactically incorrect queries would fail to compile instead of causing runtime errors. For example, forgetting the dot before adding another method call to the chain or misspelling a method's name would cause compilation to fail.

+ Type-Safety: ...

+ Tooling: As the DSL is an internal DSL with Scala being its host language, every query expressed in it is also valid Scala. As a consequence, IDEs and other tools that support Scala can also be leveraged when expressing queries using the DSL. For example, one could benefit from all of the static safeguards listed above without ever manually hitting the compile button, as IDEs such as JetBrains's IntelliJ IDEA [34] continuously perform static analysis while typing.



```
further structure
- point out advantages of dsl over unstructured strings
- show how operators can not just be applied to primitives but also to complex queries via method chaining
- show how queries can be bound to names and how these names can then be used within other queries
- show how to express qos requirements
- eventually move on to explaining the implementation
```

#### 3.4 Execution Graph
#### 3.5 Quality of Service

### 4 Simulation

### 5 Conclusion

### References

+ [1] Hinze, Sachs, Buchmann: Event-Based Applications and Enabling Technologies
+ [2] Chandy, Schulte: Event Processing - Designing IT Systems for Agile Companies
+ [3] http://www.espertech.com/products/esper.php
+ [4] https://flink.apache.org/
+ [5] https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/libs/cep.html
+ [6] Chakravarthy, Adaikkalavan: Events and Streams: Harnessing and Unleashing Their Synergy!
+ [7] Etzion, Niblett: Event Processing in Action
+ [8] 2010: Schilling, Koldehofe, Rothermel, Pletat: Distributed Heterogeneous Event Processing
+ [9] 1999. Liebig, Cilia, Buchmann. Event Composition in Time-dependent Distributed Systems
+ [10] 2003. Pietzuch, Shand, Bacon. A Framework for Event Composition in Distributed Systems
+ [11] 2003. Zeidler, Fiege. Mobility support with REBECA
+ [12] 2008. Biger, Etzion, Rabinovich. Stratified implementation of event processing network
+ [13] 2009. Farroukh, Ferzli, Tajuddin, Jacobsen. Parallel Event Processing for Content-Based Publish/Subscribe Systems
+ [14] 2009. Khandekar, Hildrum, Parekh, Rajan, Wolf, Wu, Andrade, Gedik. COLA: Optimizing Stream Processing Applications via Graph Partitioning
+ [15] 2012. Hirtzel. Partition and Compose: Parallel Complex Event Processing
+ [16] 2013. Ottenwälder, Koldehofe, Koldehofe, Ramachandran. MigCEP: Operator Migration for Mobility Driven Distributed Complex Event Processing
+ [17] 1988. Dayal, Blaustein, Buchmann, Chakravarthy, Hsu, Ledin, McCarthy, Rosenthal. Sarin. The HiPAC Project: Combining Active Databases and Timing Constraints
+ [18] 1993. Gatziu, Dittrich. SAMOS: An Active Object-Oriented Database System
+ [19] 1994. Chakravarthy, Krishnaprasad, Anwar. Composite Events for Active Databases: Contexts and Detection
+ [20] 2003. Adaikkalavan, Chakravarthy. SnoopIB: Interval-Based Event Specification and Detection for Active Databases
+ [21] DSLs in Action
+ [22] Domain Specific Embedded Compilers
+ [23] ScalaQL: Language-Integrated Database Queries for Scala
+ [24] Embedded Typesafe Domain Specific Languages for Java
+ [25] Domain-specific Languages and Code Synthesis Using Haskell
+ [26] Appel, Sachs, Buchmann. Quality of Service in Event-based Systems
+ [27] Behnel, Fiege, Mühl. On Quality-of-Service and Publish-Subscribe
+ [28] Buchmann, Appel, Freudenreich, Frischbier, Guerrero. From Calls to Events: Architecting Future BPM Systems
+ [30] Stream Data Processing: A Quality of Service Perspective
+ [31] http://slick.lightbend.com/
+ [32] https://github.com/milessabin/shapeless
+ [33] http://www.artima.com/weblogs/viewpost.jsp?thread=179766
+ [34] https://www.jetbrains.com/idea/