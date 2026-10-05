import Libraries.*
import org.typelevel.sbt.tpolecat.DevMode
import sbt.Keys.libraryDependencies

val scala2Version = "2.13.18"
val scala3Version = "3.9.0"

scalaVersion := scala2Version
homepage := Some(uri("https://github.com/PerformanceIMMO/pg-event-store"))
licenses := List("Apache-2.0" -> uri("http://www.apache.org/licenses/LICENSE-2.0"))
organization := "immo.performance"
organizationName := "Performance IMMO"
developers := List(
  Developer(
    id = "mbaechler",
    name = "Matthieu Baechler",
    email = "matthieu@baechler-craftsmanship.fr",
    url = uri("https://baechler-craftsmanship.fr/")
  ),
  Developer(
    id = "ubourdon",
    name = "Ugo Bourdon",
    email = "bourdon.ugo@gmail.com",
    url = uri("http://demon-agile.blogspot.com/")
  )
)
ThisBuild / tpolecatDefaultOptionsMode := DevMode
scalafixOnCompile := false

addCommandAlias("lint", "scalafixAll; scalafmtAll; scalafmtSbt")
addCommandAlias("check", "scalafmtCheckAll; scalafmtSbtCheck")

lazy val commonSettings = Seq(
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision,
  Compile / scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) => Seq("-Xsource:3", "-Ymacro-annotations", "-Wconf:cat=scala3-migration:info")
      case _            => Nil
    }
  }
)

lazy val root = (projectMatrix in file("."))
  .settings(name := "pg-event-store-root")
  .settings(commonSettings)
  .settings(Seq(publish / skip := true))
  .aggregate(core, `zio-json`, `play-json`, postgres, memory)

lazy val core = (projectMatrix in file("core"))
  .settings(commonSettings)
  .jvmPlatform(scalaVersions = Seq(scala3Version, scala2Version))
  .settings(
    name := "pg-event-store-core",
    libraryDependencies ++= zio ++ `zio-test`.asTest
  )

lazy val `test-suite` = (projectMatrix in file("test-suite"))
  .settings(commonSettings)
  .jvmPlatform(scalaVersions = Seq(scala3Version, scala2Version))
  .settings(
    name := "pg-event-store-test-suite",
    libraryDependencies ++= zio ++ `zio-test`
  )
  .dependsOn(
    core % "compile->compile;compile->test"
  )

lazy val postgres = (projectMatrix in file("postgres"))
  .settings(commonSettings)
  .jvmPlatform(scalaVersions = Seq(scala3Version, scala2Version))
  .settings(
    name := "pg-event-store-postgres",
    libraryDependencies ++= zio ++ `zio-test`.asTest ++ doobie ++ `postgres-test-container`.asTest ++ logback.asTest
  )
  .dependsOn(core, `test-suite` % Test)

lazy val memory = (projectMatrix in file("memory"))
  .settings(commonSettings)
  .jvmPlatform(scalaVersions = Seq(scala3Version, scala2Version))
  .settings(
    name := "pg-event-store-memory",
    libraryDependencies ++= zio ++ `zio-test`.asTest
  )
  .dependsOn(core, `test-suite` % Test)

lazy val `zio-json` = (projectMatrix in file("zio-json"))
  .settings(commonSettings)
  .jvmPlatform(scalaVersions = Seq(scala3Version, scala2Version))
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

lazy val `play-json` = (projectMatrix in file("play-json"))
  .settings(commonSettings)
  .jvmPlatform(scalaVersions = Seq(scala2Version))
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
