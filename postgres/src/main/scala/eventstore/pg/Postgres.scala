package eventstore.pg

import doobie.util.transactor
import zio.Task

object Postgres {

  case class DbConfig(
      port: Int,
      host: String,
      name: String,
      login: String,
      password: String,
      connectionPoolSize: Int,
      schema: Option[String] = None
  )

  type ZTransactor = transactor.Transactor[Task]
}
