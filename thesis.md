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

Furthermore, there is another significant difference between SP and CEP operators. As said, SP operators resemble those of relational algebra. Some relational operators, however, are blocking operators (e.g., `join`), that is, they are defined over finite sets of data and block execution until these are available in their entirety. Streams, however, can be viewed as infinite sets of data. As a consequence, in SP, those operators are not applied to streams directly. Instead, they require their operand streams to be annotated with windows that are typically expressed in terms of time or number of events. A window then only contains a finite number of events of the respesctive stream. So-called consumption modes are the counterpart of windows in CEP. The CEP operator `and` is semantically ambiguous in the sense that it only specifies the event types to be correlated but not the event instances. For example, the query `A and B` only specifies that an event of type `A` should be correlated with events of type `B`. However, given the occurences of the events `b1`, `b2`, `a1`, in that order, it is not clear whether `a1` should be correlated with `b1` or `b2`--this would depend on the consumption mode. (See [19] for more information on consumption modes.) One of the differences between windows and consumptions modes is that the former are applied to streams (i.e., operands) while the latter are applied to operators. As pointed out in [30], consumption modes "can be loosely interpreted as load shedding, used from a semantics viewpoint rather than a QoS viewpoint" as they essentially dictate which events can be dropped. To the best of my nderstanding, the same could be said about windows.

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

In this section, QoS metrics that have been identified EP solutions or related infrastructures are presented. Firstly, QoS metrics regarding such solutions in a general sense are listed. Secondly, QoS metrics that are specific to SP or CEP are described.

In [26], it is noted that "[f]uture software systems" have to be "responsive to events" in order to "adapt the software to enhance business processes". Logistics processes that must be altered according to incoming updates on traffic are mentioned as an example. Furthermore, it is stressed that in such a scenario, critical business processes are now triggered by events, thus "[t]he trust in such [...] systems depends to a large extent on the Quality of Service (QoS) provided by the underlying event system". The definition of QoS metrics as well as finding the means to monitor those is considered a "major challenge".

Since "[f]or the communication [...] the paradigm of choice is publish/subscribe" [26], it makes sense to take a look at [27], a paper called "On Quality-of-Service and Publish-Subscribe" in which QoS metrics for publish/subscribe-based systems are identified. It is to be noted, however, that in [26], it is recommended to first identify "functionality needs" of an EP solution and then, based on this, compile a list of QoS requirements--rather than building a Swiss army knife EBS supporting all imaginable [...] QoS needs". Nevertheless, the following paragraphs summarize some of the generic QoS metrics presented in [27]. Before that, a short introduction to the publish/subscribe paradigm, also from [27], is quoted:

> "The system model of the publish-subscribe communication paradigm is surprisingly simple. [... There are] three roles: publishers, subscribers and brokers. Publishers [...] provide information, advertise it and publish notifications about it. Subscribers [...] specify their interest and receive relevant information when it appears. Brokers mediate between the two by selecting the right subscribers for each published notification. [...] There can be a single centralized broker, a cluster of them or a distributed network of brokers."

Although the QoS metrics listed below were put together "in the context of distributed and decentralized publish-subscribe systems", they also apply to EP solutions, even to those that are not based on the former. The publishers, subscribers and brokers found in a publish/subscribe system can be thought of event producers, event consumers and EPAs as found in an EPN, respectively. However, whenever something is stated specifically about publishers and subscribers, with regards to EP solution, this does not only apply to event consumers and event producers, but also to EPAs, as they also cosume and produce event streams.

+ Latency: It is stated that "[s]ubscribers request a publisher that is within a maximum latency bound". An "end-to-end latency" between a publisher and a subscriber "depends on the number of [...] hops between them" as well as on the time each broker takes to deal with a notification. It is stated that in a distributed setting, "measured lower bounds" may at best "give hints" about whether some latency requirement can be met--not "absolute guarantees". Preallocating paths is mentioned as a way to deal with latency requirements.
+ Bandwith: It is denoted that publishers announce upper/lower bounds regarding the stream they produce, whereas subscribers "restrict the maximum stream of notifications they want to receive". It is recommended to consider bandwidth at the "per-broker" level. Given that "each broker knows the bandwidth it can make locally available to the infrastructure", an upper-bound for an entire path can be estimated, allowing for routing "based on the highest free bandwidth".
+ Message priorities: It is proposed that publishers annotate the notations they produce with relative or absolute priorities, denoting their importance compared to other notifications produced either by themselves or elsewhere, respectively. Subscribers, on the other hand, specify priorities regarding their subscriptions. At the "per-broker" level, priorities "can be used to control the local queues of each broker". This will result in the "end-to-end application" of the priorities along an entire path. This is commonly implemented by letting notifications with higher priorities overtake those with lower priorities at each broker.
+ Delivery guarantees: While subscribers announce "which notifications they must receive", and where they are sensitive to duplicates, it is stated that publishers "specify if subscribers must receive certain notifications". A simple approach that is mentioned is to simply let the system know which messages can be dicarded. More sophisticaed approaches concern "the completeness and duplication of delivery", i.e., a subscriber may receive each notification that is directed to it "at least once", "at most once", or "exactly once". While meshing is listed as a way to achieve "at least once", it comes at the price of "increasing the message overhead". Disconnected subscribers are "[a]nother problem", as they might stay disconnected, in which case "at least once" cannot be guaranteed, no matter how long the notifications are buffered.
+ Notification Order: It is explained that "[t]he order in which notifications arrive" is of significance in some cases while it is not in other cases. With centralized ordering, "ordering is [considered] easy to achieve". However, "distributed ordering of events coming from different sources" is described as a problem. Deploying a "central broker to enforce a global odering" is mentioned as a "generic approach" to tackle this challenge, imposing a "limit" on the "scalability of the overall infrastrucure", though.
+ Validity interval: The importance of the infrastructure knowing "how long a notification stays valid" is stressed. It is explained that this is specified either "in terms of time" of "by the arrival of later messages". Examplary, it is described as an "efficient approach" to specify that "only the most recent event is of interest" through "follow-up messages" as this allows for the infrastructure to "shorten its queues in high traffic situations".

The above list of QoS metrics obviously applies to both stream processing and complex event processing. (As said, brokers are to be thought of as event processing agents, and it has not been specfied whether these EPAs perform SP or CEP operations.) There are, however, QoS metrics that specifically apply to either SP or CEP. Some of them have been identified in [28].

Among others, the following SP-specific QoS metrics are listed in the section "QoS of Stream Processing".

+ Timeliness: It is stated that applications relying on SP "typically have timing contraints to meet", thus the timeliness of the "continuous processing of incoming events" is considered "one of the most relevant QoS requirements". According to the authors, many of these applications may "tolerate approximate results", resulting in a "common" "trade-off"--"accuracy for timeliness". To the best of my understanding, an example for this trade-off would be favoring some less precise filter that therefore is also less time-consuming over some more precise and more time-consuming filter.
+ Throughput: "[A]chievable throughput" is considered a "closely related QoS metric". SP solutions are said to "attempt to optimize" their "continuous query execution" in order to achieve maximum thoughput. As load shedding is regarded as "often" being the "only practical approach", the result is, again, a "trade-off of accuracy for timeliness". At this point, it is noted that the expected timeliness/accuracy must be specified when the business process reliying on the SP solution is designed.
+ Order: Whether events may be processed out of order is listed as an "application dependent" "issue".

In the section "QoS of Event Composition", CEP-specific QoS metrics are listed, some of them are listed below:

+ Order: Here, establishing an "odering between events" can be considered the most important QoS metric, as "achievable QoS [...] depends lagely on the possibility" to be able to do so. As an example of an operator requiring events to be ordered, the `sequence` operator--"which is part of most event algebras"--is mentioned. Furthermore, it is stated that the "natural ordering" is time-based, which, according to the authors, is no problem if "there is only one central clock" as well as only one pervent occuring every clock tick. If, however, there may be "multiple events" occuring at the same point in time, being "time-stamped by different clocks", establishing a total order is said to be impossible. Lastly, it is explained that the granularity of timestamps plays a major role when it comes to timestamping: Events that might have been distinguishable with fine-grained timestamps might no be distinguishable when being timestamped more coarsely.
+ Delay/loss of messages: The delay or loss of messages is described as a "source of ambiguity". As an example, it is explained that it is impossible to determine that an event "did not occur in a given interval" unless it can be asserted that the event in question is neither delayed nor lost. With regard to bounded networks, the authors refer to the 2g-precedence model. (An explanation of this model can be found in [9].) With regard to unbounded networks--"such as the internet"--they refer to [9], i.e., an approach based on the the injection of "heartbeat events from an outside time-service" that assumes that events "in the same channel do not overtake each other".

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
+ [26] Quality of Service in Event-based Systems
+ [27] On Quality-of-Service and Publish-Subscribe
+ [28] From Calls to Events
+ [30] Stream Data Processing: A Quality of Service Perspective