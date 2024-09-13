import sbt.Keys.libraryDependencies
import org.typelevel.sbt.tpolecat.DevMode
import Libraries.*
import xerial.sbt.Sonatype.sonatypeCentralHost

ThisBuild / scalaVersion := "2.13.14"
ThisBuild / homepage := Some(url("https://github.com/PerformanceIMMO/pg-event-store"))
ThisBuild / licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / organization := "immo.performance"
ThisBuild / organizationName := "Performance IMMO"
ThisBuild / scalacOptions := Seq("-Xsource:3", "-Ymacro-annotations")
ThisBuild / developers := List(
  Developer(
    id = "mbaechler",
    name = "Matthieu Baechler",
    email = "matthieu@baechler-craftsmanship.fr",
    url = url("https://baechler-craftsmanship.fr/")
  )
)
ThisBuild / tpolecatDefaultOptionsMode := DevMode
ThisBuild / scalafixOnCompile := false
ThisBuild / sonatypeCredentialHost := sonatypeCentralHost

addCommandAlias("lint", "scalafixAll; scalafmtAll; scalafmtSbt")
addCommandAlias("check", "scalafmtCheckAll; scalafmtSbtCheck")

lazy val commonSettings = Seq(
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision
)

lazy val root = (project in file("."))
  .settings(name := "pg-event-store-root")
  .settings(commonSettings)
  .settings(Seq(publish / skip := true))
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
