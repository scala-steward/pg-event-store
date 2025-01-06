import sbt.*

object Libraries {
  val zio_version = "2.1.11"
  val doobie_version = "1.0.0-RC5"

  val newtypes = Seq(
    "io.estatico" %% "newtype" % "0.4.4"
  )

  val zio = Seq(
    "dev.zio" %% "zio" % zio_version,
    "dev.zio" %% "zio-streams" % zio_version
  )

  val `zio-test`: Seq[ModuleID] = Seq(
    "dev.zio" %% "zio-test" % zio_version,
    "dev.zio" %% "zio-test-magnolia" % zio_version,
    "dev.zio" %% "zio-test-sbt" % zio_version
  )

  val `zio-json-libs` = Seq(
    "dev.zio" %% "zio-json" % "0.7.3"
  )

  val doobie = Seq(
    "dev.zio" %% "zio-interop-cats" % "23.1.0.3",
    "org.tpolecat" %% "doobie-core" % doobie_version,
    "org.tpolecat" %% "doobie-postgres" % doobie_version,
    "org.tpolecat" %% "doobie-hikari" % doobie_version
  )

  val logback = Seq(
    "ch.qos.logback" % "logback-classic" % "1.5.16"
  )

  val `play-json-libs` = Seq(
    "com.typesafe.play" %% "play-json" % "2.9.4",
    "org.julienrf" %% "play-json-derived-codecs" % "7.0.0"
  )

  val `postgres-test-container` = Seq(
    "com.dimafeng" %% "testcontainers-scala-postgresql" % "0.41.5"
  )

  implicit class TestOps(libs: Seq[ModuleID]) {
    def asTest: Seq[ModuleID] = libs.map(_ % Test)
  }
}
