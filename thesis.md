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
+ [3 EventScala Framework](#3-eventscala-framework)
    + [3.1 Overview](#31-overview)
    + [3.2 Domain-specific Language](#32-domain-specific-language)
    + [3.3 Operator Graph](#33-operator-graph)
    + [3.4 Quality-of-Service Monitors](#34-quality-of-service-monitors)
+ [4 Simulation](#4-simulation)
+ [5 Conclusion](#5-conclusion)
+ [References](#references)

### 1 Introduction

### 2 State of the Art

#### 2.1 Overview

Event processing (EP) has, according to Hinze, Sachs and Buchmann, become the "paradigm of choice" in "many monitoring and reactive applications", with application scenarios including traffic monitoring, fraud detection, supply chain management and many more. [1]

This section is structured as follows. Firsty, widely accepted terminology and concepts are described. Secondly, some EP solutions as well as the respective reseach papers are referenced.

Chandy and Schulte simply describe an event as "something that happens". [2] In [1], however, two more refined notions of events are introduced: "change events" (e.g., an object changing its position) and "status events" (e.g., a value yielded by a sensor).

After being observed and signalled, an event takes takes the form of an event instance, which corresponds to an event type. An event instance is commonly represented as a tuple of values, with the type of each element of the tuple being defined in the associated event type. For example, a temperature reading from a sensor "X" indicating 21 degrees celsius might be represeted by the event instance `("X", 21)` with the event type being `(String, Int)`. The remainder of this text will refer to event instances as events.

Analoguous to expressions in programming languages, which are either primitive values (e.g., `true`, `42`) or made up of other expressions combined by operators and functions (e.g., `true && isThisAGoodNumber(42)`), events may be primitve events or compositions/derivations of primitive and/or other composite/derived events. "Composite events" are "aggregations of events". "Derived events" are "caused by other events and often are at a different level of abstraction", e.g., a series of failed login attemps might cause an intrusion event. [1]

Etzion and Neblett define an "event stream (or stream)" as a "set of associated events" that is "often" "temporally ordered". A stream solely consisting of events of the same type  is called a "homogeneous stream" as opposed to "heterogenous". [7]

Operators are defined over (heterogenous) streams as opposed to individual events. The `or` operator, for example, represents the union of two streams, and places the events of both streams in one result stream. The two streams the `or` operator takes as operands can be viewed as its incoming streams, the result stream can be viewed as its outgoing stream.

Traditionally, two approches to event processing can be distinguished.

+ Stream processing (SP) typically features operators that resemble those of relational algebra, e.g., `projection`, `selection`, `join`, etc. SP queries are usually expressed in some SQL dialect and are commonly called "continuous queries". (This term underlines the following inversion of principles: In traditional DBMSs, data is being persisted as opposed to queries. Continuous queries, however, are being persisted and run *continuous*ly, while data is flowing through.)
+ Complex event processing (CEP) typically features operators that resemble those of boolean algebra, e.g., `and`, `or`, `not`. Operators such as `sequence` and `closure` are also common. CEP queries are usually expressed using rule languages.

Most solutions somehow do feature the operators of both approaches, though. Esper, for instance, can be considered a SP engine and comes with a typical SQL dialect, EPL (Event Processing Language). [3] Queries made up of CEP operators can, however, be expressed using so-called "patterns". These can then be used as operands of SP operators. (Listing 1.a) It is not possible to use SP operators within a pattern, though. (Listing 1.b) Another solution, Apache Flink, which considers itself to be a "stream processing framework", features typical CEP operators (e.g., sequence as `followedBy`) in a designated library, FlinkCEP [4, 5].

Listing 1.a: In EPL, the `join` operator (`,`) can be applied to a `pattern`.
```sql
select * from Sensor1.std:lastEvent(), pattern[every (Sensor2 or Sensor3)].std:lastEvent()
```

Listing 1.b: On the contrary, in EPL, the `join` operator cannot be used within a `pattern`.
```sql
// Invalid EPL!
select * from pattern[every (Sensor1 or (Sensor2.std:lastEvent(), Sensor3.std:lastEvent()))]
```

It is to be pointed out that the distinction between SP and CEP is blurry, as many books and publications often use the terms SP and CP in their borader sense, that is, EP in general. SP and CEP do, however, pose different challenges when it comes to quality of service. (See section 2.4.) Refer to section 5 ("Analysis of Event vs. Stream Processing") of [6] for a more detailed comparison of CEP an SP.

In [2], the term event-driven architecture (EDA) is defined as the "concept of being event-driven", i.e., acting in response to an event, being applied to software. The following "five principles of EDA" are identified:

+ Individuality: Each event is transmitted individually, not as part of a batch.
+ Push: Events are pushed, not pulled, i.e., requested.
+ Immediacy: Events are being reacted to immediately after reception.
+ One-way: The type of communication is "fire-and-forget", events are neither being acknowledged nor replied to.
+ Free of command: An event never prescribes the action that will be taken upon reception.

Early EP solutions are HiPAC [17], SAMOS [18], Snoop [19] and SnoopIB [20]. To the best of my knowledge, these will give an impression of the developments from the late 1980s to the early 2000s. Up-to-date solutions are, for example, Esper [3] and Flink [4].

#### 2.2 Language Integration

This section starts out by examining and criticizing the way queries are typically communicated to EP solutions. Then, the notion of domain-specific languages (DSLs) is introduced. Lastly, it is described how DSLs are used to ease the aforementioned problems in the context of relational databases.

As pointed out in [8], "there does not exist any generally accepted definition language for complex event processing". However, many EP solutions--especially those that rely on dialects of SQL for expressing queries, e.g., Esper--receive the latter in the form of strings, just as traditionally done in DBMSs.

In [22], it is laid out why communicating SQL expressions in the form of "unstructured strings" is a bad approach. Firstly, "[p]rogrammers get no static safeguards against creating syntactically incorrect or ill-typed queries, which can lead to hard to find runtime errors". Furthermore, it is noted that the programmer at least needs to know the SQL dialect as well as the "language that generates the queries and submits them". To underline this very point, the authors of [24] present "a very simple example of an SQL query in Java" that contains a few errors. The SQL query is embedded in Java as a `String`. Among other errors, it contains a misspelled SQL command. Furthermore, it is statet that the assumtions made about the type of the column and the result set could be wrong. The problem is that, as described in [23], the query is embedded "within application code in the form of raw character strings". As "[t]hese queries are unparsed and completely unchecked until runtime", malformed queries do not cause the code they are embedded in to not compile but will cause trouble when being executed run-time.

To tackle this issue with regards to relational DBMSs, the contribution of [23] is a so-called domain-specific language (DSL)--named ScalaQL--as a way "to make the host language compiler [i.e., scalac] aware of the query and capable of statically eliminating these runtime issues". (In fact, the contribution of the much earlier released publication [22] is very similar. Here, the host language is Haskell, however, the problem domain is also relational databases. The authors of [22] seem to be the first ones to "show how to embed the terms and type system of another (domain-specific) programming language into the Haskell framework, which dynamically compzutes and executes programs written inthe embedded language.

To explain what DSLs are, it makes sense to turn to the book "DSLs in Action" [21], which appears to be the standard primer on the subject. In it, one can find the following definition:

> "A DSL is a programming language that's targeted at a specific problem; other programming languages that you use are more general purpose. It contains the syntax and semantics that model concepts at the same level of abstraction that the problem domain offers."

Furthermore, it is stated that--"[m]ore often than not"--DSLs are used by non-expert programmers. For this to be possible, it is necessary that the DSL appeals to its target group by having "the appropriate level of abstraction", that is, it must embody the particular terminology, idioms, etc. of the respective domain. This, however, leads to so-called "limited expressivity--"you can use a DSL to solve the problem of that particular domain only". Exemplary, the author mentions that "[m]athematicians can easily learn and work with Mathematica" and "UI designers feel comfortable writing HTML". Regarding limited expressivity, it is noted that "[y]ou can't build cargo management systems using only HTML".

In [21], the notion of DSLs is further differentiated. Accordingly, there are internal as well as external DSLs, with internal DSLs also being know as embdedded DSLs. They are defined as follows:

> "An internal DSL is one that uses the infrastructure of an existing programming language (also called the host language) to build domain-specific semantics on top of it."

> "An external DSL is one that's developed ground-up and has separate infrastructure for lexical analysis, parsing technologies, interpretation, compilation, and code generation.

In [22], it is argued that internal DSLs "expressed in higher order, typed [...] languages" are better suited as a "framework for domain-specific abstractions" than external DSLs. This is, according to the authors, due to the fact that programmers only have to know the host language as domain specific abstractions are made available as "extension languages". Other advantages mentioned are the possibility to leverage other "domain-sprecific libraries" as well as the possibility to use the host language's infrastructure, e.g., its module and type system. In [24], more advantages of internal DSLs are stated, e.g., "re-using the platform tooling, which in Java case includes compilers, advanced IDEs, debuggers, profilers, and so on". Furthermore, it is states that development is much easier, as it essentially "boils down to writing an API".

Finally, it is to be noted that Scala lends it self very well as a host language for internal DSLs. Scala language features used to develop ScalaQL include "operator overloading, implicit conversions, and controlled call-by-name semantics" [4]. With ScalaQL (alongwith other existing solutions) renders SQL strings to communicate with DBMSs deprecated in the Scala ecosystem, it is the logical next step to render SQL-like strings to communicate with EP solutions deprecated in the Scala ecosystem--by developing an internal DSL.

#### 2.3 Distributed Execution

In [8], Schilling, Koldehofe, Rothermel and Pletat argue that "distributing the handling of events" is of growing importance, if only due to "the emerging increase in event sources". It can be stated that other reasons for this trend are the need to exploit parallelism to meet increasing efficiency demands, avoiding single points of failure, and many more.

Academia has, in fact, proposed numerous approaches to distributed event processing, e.g, [8 - 16]. To the best of my knowledge, however, these have too few characteristics in common to distill one reference model representing academia's approach to distributed EP. Furthermore, as pointed out in [8], "there exists a gap between [...] academia and the industry", that is, none of the approaches to distributed EP proposed are actually picked up and used in practice. One of the stated reasons for this circumstance is that EP technology "in business applications" needs "to access context information related to business processes" that "often resides in centralized databases". As a consequence of academia's propositions being too diverse to be adequately represented here as well as the lack of solutions originating in industry, this section is structured as follows. Firstly, distributed EP is introduced as an abstraction, and secondly, challenges that are present in this area of research are listed, along with references to publications that address these.

In [7], Etzion and Niblett introduce an abstaction called event processing network (EPN). An EPN is made up of processing elements. It is represented as a graph with the processing elements being the nodes of the graph. Nodes can have "input terminals" and/or "output terminals", while the former may be connected to the latter, forming the edges of the graph. An edge shows "the flow of event instances through the network", thus can be considered a stream. There are the following types of processing elements:

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

It is to be stressed that an EPN diagram is an abstraction, comprised of "platform-independend definition elements", that is not assuming a distributed implementation. After the introduction of the EPN concept, the authors lay out the "[i]mplementation perspective", where the graph has to be materialized into what is being called "runtime artifacts", resulting in a "runtime system". Usually, there is no "one-to-one correspondence" between the processing elements of an EPN and the runtime artifacts of a respective implementation. With regards to this, two "extremes" are being described:

+ The entire EPN may be represeted by one runtime artifact, i.e., one centralized runtime system.
+ Each EPA is represented by one runtime artifact. These distribute events between each other and "can be placed on different server[s], allowing much of the [...] work to be performed in parallel."

To the best of my knowledge, the second scenario can be considered a good example for distributed EP, even though there are obviously many ways to map EPAs to runtime artifacts, several of which might also be considered distributed approachs.

In the chapter "EPA assignment optimization" in [7], the authors mention parallel and distributed processing. Parallel processing is considered "[o]ne of the major ways to achieve [...] performance metrics". Then, three levels of parallelism are introduced, that is, using multiple threads in one core, using multiple cores and using multiple machines. However, finding out "which activities should be run in parallel" is listed as a "difficult" challenge. Regarding distributed processing, it is stated that "moving the processing close to the producers and consumers" constitutes an optimization method. Also, the practice of grouping EPAs for them to "to execute together for better performance" is explained and called "key" to both parallel execution and distributed execution. "Stratification", a partitioning approach in which EPAs are assigned to so-called "strata" is then explained: "If `EPA1` produces events that are consumed by `EPA2`, then `EPA2` is placed in a higher stratum."

```
todo
add illustration
```

Regarding the aforementioned challenge regarding parallelization, that is, figuring out which parts of the event processing may run in parallel, [12 - 15] present different approaches. Furthermore, the previously mentioned "moving [of] the processing close to the producers and consumers", poses another challenge, especially with mobile producers/consumers. In [10, 11, 16], interesing approaches with regards to this are presented. Moreover, it appears that a good amount of the solutions to distributed EP proposed by academia, e.g., [9, 10, 11, 13], are built upon loosely-coupled publish/subscribe systems (or pub/sub systems), which constitue a related field of research themselves. Also, for events to be partitioned into time-based windows (as sometimes required by the SP operator `join`) or for them to be ordered propery (as demanded by the CEP operator `sequence`), they need to be properly timestamped. Timestamping is a well-studied challenge in distributed systems in general and many approaches to tackle it have been proposed. [9] presents an interesting solution specifically aimed at distributed EP, leveraging a combination of NTP-synchronized local clocks and heartbeat events.

#### 2.4 Quality of Service

+ **Quality of Service in Event-based Systems**
    + Future software systems must be responsive to events and must be able to adapt the software to enhance business processes. Examples are production and logistics processes that must be rescheduled based on relevant traffic information. The trust in such reactive systems depends to a large extent on the Quality of Service (QoS) provided by the underlying event system.
    + Since the invocation of business critical processes is now triggered by events, the QoS of the event mechanism becomes a key aspect. Defining QoS and developing the necessary monitoring mechanisms is a major challenge.
    + For the communication between producers and consumers the paradigm of choice is publish/subscribe, allowing a decoupled many-to-many communication. The pub/sub middleware must provide a certain QoS [8]. At present, one of the most widely used technologies providing pub/sub capabilities is the Java Message Service (JMS).
    + Generally, QoS has not been addressed extensively in research; a basis is provided in [4] where basic QoS metrics (e.g., latency, bandwidth, delivery guarantees) are discussed.
    + Therefore, we used a list of EBS features as a starting point to determine the functionality needs of EBSs. From them we derived the QoS requirements. [...] Thus, rather than building a Swiss army knife EBS supporting all imaginable features and QoS needs, the requirements are matched during the requirements engineering and development process. [...] A comprehensive understanding of QoS is only possible after all required features have been identified.

+ **On Quality-of-Service and Publish-Subscribe**
    + In this paper, we provide a broad overview of relevant quality-of-service metrics and describe their specific meaning in the context of distributed and decentralized publish-subscribe systems.
    + The system model of the publish-subscribe communication paradigm is surprisingly simple. The figure shows its three roles: publishers, subscribers and brokers. Publishers (aka producers) provide information, advertise it and publish notifications about it. Subscribers (aka consumers) specify their interest and receive relevant information when it appears. Brokers mediate between the two by selecting the right subscribers for each published notification.
    + An important property of the publish-subscribe model is the level of abstraction at which publishers and subscribers communicate. They are not aware of organization and size of the system. There can be a single centralized broker, a cluster of them or a distributed network of brokers. All that clients see is their specific brokers through which communication partners are self-selecting by interest.
    + To this end, we survey the relevant quality-of-service metrics and describe their meaning within the very specific context of the publish-subscribe model.
    + QoS at the Global Infralstrucutr Level (End-to-end latency, bandwidth and delivery guarantees form low-level properties of the broker infrastructure. In a centralized infrastructure in which the client connections also implement QoS, there are ways to impose hard limits on them. In any less predictable environment, especially distributed multi-hop infrastructures, they should not be understood as real-time guarantees. Here they become probabilistic options or even hints about preferences of clients.
        + Latency: The end-to-end latency between producers and consumers depends on the number of broker hops between them, the travel time from hop to hop and the time it takes each broker to forward a notification. In a centralized system, the minimum travel time between broker and clients provides a hard lower bound and the additional forwarding time depends on the broker load. Even in a distributed broker infrastructure, measured lower bounds can give hints if a requested QoS level is achievable at all. In general, however, they do not allow for absolute guarantees. One way of dealing with latency requirements is by preallocating fixed paths between senders and receivers. This avoids the overhead of having to establish connections on request and allows for meaningful predictions.
        + Bandwidth: Producers specify upper/lower bounds for the stream they produce. Subscribers restrict the maximum stream of notifications they want to receive. [...] Therefore, bandwidth requirements should rather be regarded at a per-broker level. If each broker knows the bandwidth that it can make locally available to the infrastructure, this gives an upper bound for the throughput of a path. Although not necessarily accurate, such an upper bound allows to route notifications based on the highest free bandwidth on the neighbouring brokers. It can be used to avoid high-traffic paths and to do local traffic optimization.
        + Message priorities: Priorities between notifications can be used to control the local queues of each broker which will eventually lead to their end-to-end application along a path. Again, channels can be used to shorten the path for high-priority notifications, the extreme case being to send them directly from the system entry point to the recipients. More commonly, however, notifications will be allowed to overtake those with a lower priority during the forwarding and filtering process at each broker, subject to a weighted scheduling policy. This underlines the importance of their per-broker part.
         + Implementations can be as simple as a tag for notifications that may be dropped on the delivery path. [...] More demanding guarantees regard the completeness and duplication of delivery. Subscribers can receive notifications at least once, at most once or exactly once, the latter being the combination of the first two. [...] If the infrastructure is not reliable itself, the first requirement can be achieved by meshing which increases the delivery probability at the cost of generating duplicates and thus increasing the message overhead. [...] Another problem with delivery guarantees regards temporarily disconnected subscribers [6], e.g. in a mobile environment. The broker infrastructure must store notifications that were guaranteed to be delivered at least once until the subscribers become available again. Note that this may be after an arbitrarily long time or never, so in practice, the infrastructure will only store notifications for a sensible time interval.
    + QoS at the Notification and Subscription Level
        + Notification Order: The order in which notifications arrive may or may not be relevant. In many cases, ordering is easy to achieve by either using centralized ordering, ordered transports (ATM, TCP) or by letting the producer (or its broker) impose an explicit order and sorting the notifications on delivery. [...] The distributed ordering of events coming from different sources is another problem. For content-based subscriptions, it is even hard to define a meaningful ordering in this case. It is therefore largely dependent on the subscription language and the application if such an order is applicable. A generic approach is the deployment of a dedicated, central broker to enforce a global ordering, which can in turn limit the scalability of the overall infrastructure.
        + Validity interval: It is important for the infrastructure to know how long a notification stays valid, either specified in terms of time or infered by the arrival of later messages. [...] If only the most recent event is of interest, the validity specification by follow-up messages is a particularly efficient approach. It allows the infrastructure to reorder and shorten its queues in high traffic situations.

+ **From Calls to Events**
    + QoS of Stream Procesing
        + While the operators used in stream processing resemble the operators of relational algebra but applied to windows (when the event objects are tuples), the nature of streams is quite different from the nature of data in a database. Event objects in a stream arrive continuously and are usually processed in arrival order.
        + Stream processing requires continuous processing of incoming event objects. Since applications depending on stream processing typically have timing constraints to meet, the timeliness of a response is one of the most relevant QoS requirements. Many applications that depend on stream processing can tolerate approximate results. Therefore, it is common to trade-off accuracy for timeliness. 
        + A closely related QoS metric is the achievable throughput. Stream processing systems attempt to optimize continuous query execution to maximize throughput. However, load shedding is often the only practical approach, resulting in a trade-off of accuracy for timeliness. Application processes must be aware of this and specifications of expected timeliness and tolerable accuracy are required during business process design.
        + Another issue that is application dependent is the tolerance to events being processed out of order.
    + QoS of Event Composition
        + The achievable QoS in event composition depends largely on the possibility to establish an ordering between events. While operators such as intersection and union do not require ordering, the sequence operator, which is part of most event algebras, requires an ordering of events. The natural ordering is done on time. This is perfectly fine if there is only one central clock and at most one event can occur per clock tick. As soon as multiple events can occur simultaneously and are time-stamped by different clocks it becomes impossible to establish a total order. The granularity of time is also important when trying to establish an ordering.
Two events with distinguishable order with timestamps of fine granularity (e.g. milliseconds) may not be distinguishable with coarser timestamps (e.g. seconds).
        + The delay or loss of messages, especially in wide area networks, is another source of potential ambiguity. To evaluate the negation operator, i.e. to determine whether an event did not occur in a given interval, one must be able to establish that the message with the notification is neither lost nor delayed. In networks with bounded delay, the 2g precedence model is adequate [15]. It establishes that anything outside an interval formed by one maximum delay before to one maximum delay after a given point in time can be known with certainty. For unbounded networks, such as the Internet, an approach based on sweeper events has been proposed [16]. It does not assume ordering, but requires only that two events in the same channel do not overtake each other. By injecting the heartbeat events from an outside time service, the recipient knows that everything coming over that channel after the heartbeat must be younger.













### 3 EventScala Framework

#### 3.1 Overview

#### 3.2 Domain-specific Language

#### 3.3 Operator Graph

#### 3.4 Quality-of-Service Monitors

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