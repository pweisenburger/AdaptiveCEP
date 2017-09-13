name := "EventScala"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.12.1"

resolvers ++= Seq(
  Resolver.bintrayRepo("rmgk", "maven"),
  Resolver.bintrayRepo("pweisenburger", "maven")
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"   % "2.4.16",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.16"  % "test",
  "com.espertech"     %  "esper"        % "5.5.0",
  "org.scalatest"     %% "scalatest"    % "3.0.1"   % "test",
  "de.tuda.stg" %% "rescala" % "0.20.0-SNAPSHOT"
)
