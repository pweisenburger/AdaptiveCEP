# EventScala: A Type-Safe, Distributed and Quality-of-Service-oriented Approach to Event Processing

## Bachelor Thesis of Lucas Bärenfänger

### Table of Contents
+ [1 Introduction](#1-introduction)
+ [2 State of the Art](#2-state-of-the-art)
	+ [2.1 Overview](#2.1-overview)
	+ [2.2 Language Integration](#2.2-language-integration)
	+ [2.3 Distributed Execution](#2.3-distributed-execution)
	+ [2.4 Quality of Service](#2.4-quality-of-service)
+ [3 EventScala Framework](#3-eventscala-framework)
    + [3.1 Overview](#3.1-overview)
	+ [3.2 Domain-specific Language](#3.2-domain-specific-language)
	+ [3.3 Operator Graph](#3.3-operator-graph)
	+ [3.4 Quality-of-Service Monitors](#3.4-quality-of-service-monitors)
+ [4 Simulation](#4-simulation)
+ [5 Conclusion](#5-conclusion)
+ [References](#references)

### 1 Introduction

### 2 State of the Art

#### 2.1 Overview
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

### Some GitHub Markdown syntax (TO BE DELETED)

```scala
val q: Query4[Int, String, String, Int] = 
  stream[Int, String]("A")
  .join(
    stream[String, Int]("B"),
    slidingWindow(3.seconds),
    tumblingWindow(3.instances))
```

![EventScala](img/logo.png)
