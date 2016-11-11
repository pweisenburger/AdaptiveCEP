lazy val commonSettings = Seq(
  organization := "com.scalarookie",
  version := "0.0.1-SNAPSHOT",
  scalaVersion := "2.11.8",
  addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
)

lazy val eventscala = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "eventscala",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
  ).aggregate(macros).dependsOn(macros)

lazy val macros = (project in file("macros")).
  settings(commonSettings: _*).
  settings(
    name := "eventscala-macros",
    libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-reflect" % _)
  )

lazy val demos = (project in file("demos")).
  settings(commonSettings: _*).
  settings(
    name := "eventscala-demos",
    libraryDependencies += "com.espertech" % "esper" % "5.4.0"
  ).dependsOn(eventscala, macros)
