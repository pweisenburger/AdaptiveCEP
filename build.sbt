name := "AdaptiveCEP"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.12.1"

resolvers += Resolver.bintrayRepo("stg-tud", "maven")

lazy val secureScalaProject = RootProject(uri("https://github.com/allprojects/securescala.git"))
lazy val root = (project in file(".")).dependsOn(secureScalaProject)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"   % "2.5.22",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.22"  % "test",
  "com.typesafe.akka" %% "akka-stream" % "2.5.22",
  "com.espertech"     %  "esper"        % "5.5.0",
  "org.scalatest"     %% "scalatest"    % "3.0.1"   % "test",
  "de.tuda.stg" %% "rescala" % "0.24.0",
  "com.typesafe.akka" %% "akka-cluster" % "2.5.22",
  "com.typesafe.akka" %% "akka-cluster-tools" % "2.5.22"
)

//test in assembly := {}
//
//mainClass in assembly := Some("adaptivecep.privacy.sgx.Server")
//
//assemblyJarName in assembly := "sgx-server.jar"