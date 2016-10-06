# EventScala

![EventScala](logo.png)

## Overview

EventScala is a domain-specific language implemented in Scala for expressing queries for event processing engines. It provides a compiled and type-safe alternative to traditional string-based SQL-like queries and is designed to play well with an existing setup, e.g., Esper.

It is to be noted that EventScala is a "toy language" that is part of the bachelor thesis of Lucas Bärenfänger. Its actual purpose is not to provide a feature-complete alternative to existing solutions but rather to be simple and easy-to-extend for further research. The eventual goal of the aforementioned thesis is to explore how to augment queries with quality-of-service requirements and to propose a modified version of EventScala which supports such functionality.

## Usage

EventScala requires Scala 2.11.8 as well as sbt to be installed. Download the repository and navigate to the root directory (where `build.sbt` is located). Then, do `sbt test` to make sure everything is working fine.

It is recommended to play around with the demo to get a feel for how EventScala works. In the root directory, run:

    > sbt
    > project demos
    > run

You will see the sample query `select (A.id, B.id. C.id) { A() -> B() -> C() }` executing. To run other sample queries, open up the source file of the demo and choose from existing queries that have been placed there for demonstration purposes. The path to the file is:

    demos/src/main/scala/com/scalarookie/eventscala/backend/esper/Esper.scala
    
To get a feel for how EventScala works under the hood, it is recommended to take a look at the unit tests:

    src/main/scala/com/scalarookie/eventscala/tests/DslTests.scala
    src/main/scala/com/scalarookie/eventscala/tests/BackendEsperTests.scala

## Credit

Developed by
Lucas Bärenfänger (@scalarookie)

Artwork inspired by
Maria Martin Moralejo

Visit scalarookie.com for more information.
