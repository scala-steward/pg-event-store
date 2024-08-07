package eventstore.playjson

import eventstore.EventRepository
import eventstore.pg.Postgres.ZTransactor
import eventstore.pg.PostgresEventRepositoryLive
import eventstore.playjson.pg.PostgresPlayJsonEventRepository
import play.api.libs.json.Reads
import play.api.libs.json.Writes
import zio._

object PlayJsonEventRepository {
  val postgresLayer: URLayer[ZTransactor, EventRepository[Reads, Writes]] =
    ZLayer.makeSome[ZTransactor, EventRepository[Reads, Writes]](
      PostgresEventRepositoryLive.layer,
      PostgresPlayJsonEventRepository.layer
    )
}
