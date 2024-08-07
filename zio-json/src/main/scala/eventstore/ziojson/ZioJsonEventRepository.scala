package eventstore.ziojson

import eventstore.EventRepository
import eventstore.pg.Postgres.ZTransactor
import eventstore.pg.PostgresEventRepositoryLive
import eventstore.ziojson.pg.PostgresZioJsonEventRepository
import zio._
import zio.json.JsonDecoder
import zio.json.JsonEncoder

object ZioJsonEventRepository {
  val postgresLayer: URLayer[ZTransactor, EventRepository[JsonDecoder, JsonEncoder]] =
    ZLayer.makeSome[ZTransactor, EventRepository[JsonDecoder, JsonEncoder]](
      PostgresEventRepositoryLive.layer,
      PostgresZioJsonEventRepository.layer
    )

}
