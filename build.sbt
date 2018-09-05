name := "AdaptiveCEP"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.12.1"

scalacOptions ++= Seq("-language:implicitConversions")

// only run CompleteGraphTests which groups the other tests sequentially
testOptions := Seq(Tests.Filter(_.endsWith("CompleteGraphTests")))

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"   % "2.4.16",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.16"  % "test",
  "com.espertech"     %  "esper"        % "5.5.0",
  "org.scalatest"     %% "scalatest"    % "3.0.1"   % "test",
  "com.chuusai"       %% "shapeless"    % "2.3.3"
)
