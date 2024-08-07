package eventstore.pg.test

import com.dimafeng.testcontainers.PostgreSQLContainer
import com.zaxxer.hikari.HikariConfig
import doobie.Fragment
import doobie.hikari.HikariTransactor
import doobie.util.fragment
import eventstore.pg.Postgres.DbConfig
import eventstore.pg.Postgres.ZTransactor
import org.testcontainers.utility.DockerImageName
import zio.Random
import zio.Scope
import zio.Task
import zio.UIO
import zio.ULayer
import zio.ZIO
import zio.ZIO.blockingExecutor
import zio.ZLayer
import zio.interop.catz._
import zio.test.Live

object PostgresTestUtils {

  private val dbConfig = ZIO.serviceWith[DbConfig] { config =>
    val hconfig = new HikariConfig
    hconfig.setDriverClassName("org.postgresql.Driver")
    hconfig.setJdbcUrl(s"jdbc:postgresql://${config.host}:${config.port}/${config.name}")
    hconfig.setUsername(config.login)
    hconfig.setPassword(config.password)
    hconfig.setMaximumPoolSize(config.connectionPoolSize)
    config.schema.foreach(schema => hconfig.setSchema(schema))

    hconfig
  }

  val transactorLayer: ZLayer[DbConfig, Throwable, ZTransactor] = {
    ZLayer.scoped {
      for {
        hconfig <- dbConfig
        executionContext <- blockingExecutor.map(_.asExecutionContext)
        pool <- HikariTransactor
          .fromHikariConfigCustomEc[Task](
            hconfig,
            executionContext
          )
          .toScopedZIO
      } yield pool
    }
  }

  import doobie.implicits._
  import zio.interop.catz._

  trait DbAdmin {
    def createDatabase: UIO[DbConfig]
  }

  private val dbAdminLayer: ZLayer[ZTransactor with DbConfig, Nothing, DbAdmin] = ZLayer {
    for {
      conf <- ZIO.service[DbConfig]
      transactor <- ZIO.service[ZTransactor]
    } yield new DbAdmin {
      override val createDatabase: UIO[DbConfig] =
        for {
          schemaName <- Live.live { Random.nextUUID.map(_.toString.filterNot(_ == '-')).map(name => s"schema$name") }
          _ <- createDatabaseSql(schemaName).run.transact(transactor).orDie
        } yield conf.copy(schema = Some(schemaName))
    }
  }

  private def createDatabaseSql(name: String): doobie.Update0 = (sql"""create schema """ ++ Fragment.const(name)).update

  val adminConnection: ULayer[DbAdmin] =
    ZLayer.make[DbAdmin](
      Scope.default,
      PostgresqlContainer.layer,
      PostgresqlContainer.configuration,
      transactorLayer.orDie,
      dbAdminLayer
    )

  private val newDbForTest: ZLayer[DbAdmin, Nothing, DbConfig] = ZLayer {
    for {
      admin <- ZIO.service[DbAdmin]
      conf <- admin.createDatabase
    } yield conf
  }

  private val transactorWithSchema = transactorLayer.tap { transactor =>
    ZIO
      .readFile(
        getClass.getClassLoader
          .getResource("sql_schemas/events.sql")
          .getPath
      )
      .map(fragment.Fragment.const(_))
      .flatMap(script => script.update.run.transact(transactor.get))
  }.orDie

  val transactor: ZLayer[DbAdmin, Nothing, ZTransactor] =
    ZLayer.makeSome[DbAdmin, ZTransactor](newDbForTest, transactorWithSchema)

  object PostgresqlContainer {

    val layer: ZLayer[Scope, Nothing, PostgreSQLContainer] = ZLayer {
      ZIO
        .attemptBlocking {
          val container = PostgreSQLContainer(dockerImageNameOverride = DockerImageName.parse("postgres:14"))
          container.start()
          container
        }
        .withFinalizerAuto
        .orDie
    }

    val configuration: ZLayer[PostgreSQLContainer, Nothing, DbConfig] = ZLayer {
      for { container <- ZIO.service[PostgreSQLContainer] } yield DbConfig(
        host = container.host,
        port = container.mappedPort(5432),
        name = container.databaseName,
        login = container.username,
        password = container.password,
        connectionPoolSize = 10
      )

    }
  }
}
