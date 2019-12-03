# AdaptiveCEP

AdaptiveCEP is a research project exploring ways to embed quality demands into queries for event processing systems. As such, its main contributions are:

+ **AdaptiveCEP DSL** for _expressing_ EP queries including quality demands
+ **AdaptiveCEP Akka/Esper Backend** for _executing_ EP queries including quality demands

AdaptiveCEP DSL is simply an embedded Scala DSL. AdaptiveCEP Akka/Esper Backend is a tree of Akka actors representing a query, where each actor represents a primitive or an operator of the query.

**Demo: Doing `sbt run` in the project root will cause a sample query to execute! Check it out!**


## AdaptiveCEP DSL

AdaptiveCEP DSL is the domain-specific language to express event processing queries. Queries are made up of primitives and operators and can be arbitrarily nested and composed.

Internally, queries expressed using AdaptiveCEP DSL are represented by case classes. This case class representation can then be passed to AdaptiveCEP Akka/Esper Backend for execution, but it may also be interpreted by another backend.

### Examples

+ Joining streams

    + Example 1: `join` of two streams:

    ```scala
    val q1 = streamA join streamB in (slidingWindow(3 seconds), tumblingWindow(3 instances))
    ```

    + Example 2: `join` composing a stream with another subquery:

    ```scala
    val q2 = streamC join q1 in (slidingWindow(2 instances), tumblingWindow(2 seconds))
    ```

    + Available windows:

    ```scala
    slidingWindow(x instances)
    slidingWindow(x seconds)
    tumblingWindow(x instances)
    tumblingWindow(x seconds)
    ```

+ Filtering streams

    + Example 1: Only keep those event instances `where` element 2 is smaller than 10.

    ```scala
    val q3 = streamA where { (_, value, _) => value < 10 }
    ```

    + Example 2: Only keep those event instances `where` element 1 equals element 2.

    ```scala
    val q4 = streamA where { (value1, value2, _) => value1 == value2 }
    ```

+ Quality demand

    ```scala
    val q5 =
      (streamA
        demand (frequency > ratio(3 instances, 5 seconds))
        where { (_, value, _) => value < 10 })
    ```


## AdaptiveCEP Akka/Esper Backend

As said, the AdaptiveCEP backend is a tree of Akka actors representing a query, with each actor representing a primitive or an operator of that query. Given a query as well as the `ActorRef`s to the event publishers, the backend will automatically build and start emitting events.

A node representing a primitive (so far, there's only one primitive: a subscription to a stream) is just a plain Akka actor subscribing to the respective publisher, whereas nodes representing operators are all running an independent instance of the Esper event processing engine.


# running the privacy-aware version on Azure Cloud

1. Start the instances on your Azure account and ssh to those instances
2. Follow the first time installation steps in useful_commands_AdaptiveCEP file
3. Configure the public IPs in all nodes as shown in useful_commands_AdaptiveCEP file
4. Run the leader first followed by the rest of the nodes
5. run the Main file adaptivecep.privacy.PerformanceEvaluation

## running SGX mode
1. declare implicit SGX privacy context
2. compile the Trusted Event Processor service by following the instructions in build.sbt file
3. Install SGX-LKL on one of the SGX enabled nodes, instructions can be found here (https://github.com/lsds/sgx-lkl)
4. copy the generated .jar file containing the service to sgx-lkl/apps/jvm/hello-world folder and modify the Make file to copy the sgx-service.jar with the needed configurations
5. start the trusted event processor using instructions in 'Run EventProcessor on SGX LKL.txt' file
6. configure the EventProcessorClient  to point to the public IP of the SGX-powered instance
7. run the simulation

## running PHE mode
1. declare implicit PHE privacy context
2. Deploy the cryptoServiceActor to one of the cloud instances
3. write the query using the interpreter exactly as show in the examples found under adaptivecep.privacy.PerformanceEvaluation
3. run the simulation

# Publications

Pascal Weisenburger, Manisha Luthra, Boris Koldehofe, and Guido Salvaneschi. 2017. Quality-aware runtime adaptation in complex event processing. In Proceedings of the 12th International Symposium on Software Engineering for Adaptive and Self-Managing Systems (SEAMS '17). IEEE Press, Piscataway, NJ, USA, 140-151
