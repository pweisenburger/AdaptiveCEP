# EventScala: A Type-Safe, Distributed and Quality-of-Service-oriented Approach to Event Processing
## Bachelor Thesis of Lucas Bärenfänger

### Table of Contents
+ [Introduction](#introduction)
+ [DSL](#dsl)
+ [Graph](#graph)
+ [Monitors](#monitors)
+ [Simulation](#simulation)
+ [Conclusion](#conclusion)

### Introduction

`TODO`

### DSL

`TODO`

### Graph

`TODO`

### Monitors

`TODO`

### Simulation

`TODO`

### Conclusion

`TODO`

### Sources

`TODO`

### Some GitHub Markdown syntax (TO BE DELETED)

```scala
val q: Query4[Int, String, String, Int] = 
  stream[Int, String]("A")
  .join(
    stream[String, Int]("B"),
    slidingWindow(3.seconds),
    tumblingWindow(3.instances))
```

![EventScala](logo.png)
