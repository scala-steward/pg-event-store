import sbt.Keys.libraryDependencies
import org.typelevel.sbt.tpolecat.DevMode
import Libraries._

ThisBuild / scalaVersion := "2.13.15"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.performance-immo"
ThisBuild / organizationName := "Performance IMMO"
ThisBuild / scalacOptions := Seq("-Xsource:3", "-Ymacro-annotations")

ThisBuild / tpolecatDefaultOptionsMode := DevMode
ThisBuild / scalafixOnCompile := false

addCommandAlias("lint", "scalafixAll; scalafmtAll; scalafmtSbt")

lazy val commonSettings = Seq(
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision
)

lazy val root = (project in file("."))
  .settings(name := "pg-event-store-root")
  .settings(commonSettings)
  .aggregate(core, `zio-json`, `play-json`, postgres, memory)

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "pg-event-store-core",
    libraryDependencies ++= newtypes ++ zio ++ `zio-test`.asTest
  )

lazy val `test-suite` = (project in file("test-suite"))
  .settings(commonSettings)
  .settings(
    name := "pg-event-store-test-suite",
    libraryDependencies ++= zio ++ `zio-test`
  )
  .dependsOn(core)

lazy val postgres = (project in file("postgres"))
  .settings(commonSettings)
  .settings(
    name := "pg-event-store-postgres",
    libraryDependencies ++= zio ++ `zio-test`.asTest ++ doobie ++ `postgres-test-container`.asTest ++ logback.asTest
  )
  .dependsOn(core, `test-suite` % Test)

lazy val memory = (project in file("memory"))
  .settings(commonSettings)
  .settings(
    name := "pg-event-store-memory",
    libraryDependencies ++= zio ++ `zio-test`.asTest
  )
  .dependsOn(core, `test-suite` % Test)

lazy val `zio-json` = (project in file("zio-json"))
  .settings(commonSettings)
  .settings(
    name := "pg-event-store-zio-json",
    libraryDependencies ++= zio ++ `zio-test`.asTest ++ `zio-json-libs`
  )
  .dependsOn(
    core,
    postgres % "compile->compile;test->test",
    `test-suite` % Test,
    memory % Test
  )

lazy val `play-json` = (project in file("play-json"))
  .settings(commonSettings)
  .settings(
    name := "pg-event-store-play-json",
    libraryDependencies ++= zio ++ `zio-test`.asTest ++ `play-json-libs`
  )
  .dependsOn(
    core,
    postgres % "compile->compile;test->test",
    `test-suite` % Test,
    memory % Test
  )
