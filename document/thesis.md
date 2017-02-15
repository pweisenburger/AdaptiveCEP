# EventScala: A Type-Safe, Distributed and Quality-of-Service-oriented Approach to Event Processing

## Bachelor Thesis of Lucas Bärenfänger

![EventScala](img/logo.png)

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

Chandy and Schulte simply describe an event as "something that happens". [2] In [1], however, two more refined notions of events are introduced: "change events" (e.g., an object changing its position) and "status events" (e.g., a value yielded by a sensor).

After being observed and signalled, an event takes takes the form of an event instance, which corresponds to an event type. An event instance is commonly represented as a tuple of values, with the type of each element of the tuple being defined in the associated event type. For example, a temperature reading from a sensor "X" indicating 21 degrees celsius might be represeted by the event instance `("X", 21)` with the event type being `(String, Int)`. The remainder of this text will refer to event instances as events.

Analoguous to expressions in programming languages, which are either primitive values (e.g., `true`, `42`) or made up of other expressions combined by operators and functions (e.g., `true && isThisAGoodNumber(42)`), events may be primitve events or compositions/derivations of primitive and/or other composite/derived events. "Composite events" are "aggregations of events". "Derived events" are "caused by other events and often are at a different level of abstraction", e.g., a series of failed login attemps might cause an intrusion event. [1]

Events of the same type placed in an unidirectional channel form a stream. Operators are defined over streams as opposed to individual events. The `or` operator, for example, represents the union of two streams, and places the events of both streams in one result stream. The two streams the `or` operator takes as operands can be viewed as its incoming streams, the result stream can be viewed as its outgoing stream.

Traditionally, two approches to event processing can be distinguished.

+ Stream processing (SP) typically features operators that resemble those of relational algebra, e.g., `projection`, `selection`, `join`, etc. SP queries are usually expressed in some SQL dialect and are commonly called "continuous queries". (This term underlines the following inversion of principles: In traditional DBMSs, data is being persisted as opposed to queries. Continuous queries, however, are being persisted and run *continuous*ly, while data is flowing through.)
+ Complex event processing (CEP) typically features operators that resemble those of boolean algebra, e.g., `and`, `or`, `not`.

Most solutions somehow do feature the operators of both approaches, though. Esper, for instance, can be considered a SP engine and comes with a typical SQL dialect, EPL (Event Processing Language). [3] Queries made up of CEP operators can, however, be expressed using so-called "patterns". These can then be used as operands of SP operators. (Listing 1.a) It is not possible to use SP operators within a pattern, though. (Listing 1.b) Another solution, Apache Flink, which considers itself to be a "stream processing framework", features typical CEP operators (e.g., sequence as `followedBy`) in a designated library, called "FlinkCEP" [4, 5].

Listing 1.a: In EPL, the `join` operator (`,`) can be applied to a `pattern`.
```sql
select * from Sensor1.std:lastEvent(), pattern[every (Sensor2 or Sensor3)].std:lastEvent()
```

Listing 1.b: On the contrary, in EPL, the `join` operator cannot be used within a `pattern`.
```sql
// Invalid EPL!
select * from pattern[every (Sensor1 or Sensor2.std:lastEvent(), Sensor3.std:lastEvent())]
```

It is to be pointed out that the distinction between SP and CEP is blurry, as many books and publications often use the terms SP and CP in their borader sense, that is, EP in general. SP and CEP do, however, pose different challenges when it comes to quality of service. (See section 2.4.)

In [2], the term event-driven architecture (EDA) is defined as the "concept of being event-driven", i.e., acting in response to an event, being applied to software. The following "five principles of EDA" are identified:

  - Individuality: The event producer transmits each event individually.
  - Push: Events are pushed by the event producer, not pulled, i.e., requested, by the event consumer.
  - Immediacy: The event consumer reacts to events immediately after reception.
  - One-way: The type of communication is "fire-and-forget". The event producer emits an event without and does not receive any kind of reply or acknowledgement.
  - Free of command: An event never prescribes the action the event consumer will take.

#### 2.2 Language Integration
#### 2.3 Distributed Execution
#### 2.4 Quality of Service

### 3 EventScala Framework

#### 3.1 Overview
#### 3.2 Domain-specific Language
#### 3.3 Operator Graph
#### 3.4 Quality-of-Service Monitors

### 4 Simulation

### 5 Conclusion

### References

1) Hinze, Sachs, Buchmann: Event-Based Applications and Enabling Technologies
2) Chandy, Schulte: Event Processing - Designing IT Systems for Agile Companies
3) http://www.espertech.com/products/esper.php
4) https://flink.apache.org/
5) https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/libs/cep.html


