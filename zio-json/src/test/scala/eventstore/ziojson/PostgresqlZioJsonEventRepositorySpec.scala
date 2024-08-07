package eventstore.ziojson

import eventstore.EventRepository
import eventstore.EventRepositorySpec
import eventstore.EventRepositorySpec._
import eventstore.memory.MemoryEventRepository
import eventstore.pg.test.PostgresTestUtils
import eventstore.pg.test.PostgresTestUtils.DbAdmin
import zio.Scope
import zio.ULayer
import zio.ZLayer
import zio.durationInt
import zio.json.DeriveJsonCodec
import zio.json.DeriveJsonEncoder
import zio.json.JsonCodec
import zio.json.JsonDecoder
import zio.json.JsonEncoder
import zio.test._

object ZioJsonCodecs {

  val d1: JsonCodec[Event1] = DeriveJsonCodec.gen[Event1]

  val JsonEncoderEvent1: JsonEncoder[Event1] = DeriveJsonEncoder.gen[Event1]

  val JsonEncoderEvent2: JsonEncoder[Event2] = DeriveJsonEncoder.gen[Event2]

  val d2: JsonCodec[Event2] = DeriveJsonCodec.gen[Event2]

  val JsonCodecEvent: JsonCodec[Event] =
    d1.asInstanceOf[JsonCodec[Event]].orElse(d2.asInstanceOf[JsonCodec[Event]])

  val userJsonCodec: JsonCodec[User] = DeriveJsonCodec.gen[User]

  implicit val codecs: Codecs[JsonDecoder, JsonEncoder] = Codecs(
    eventDecoder = JsonCodecEvent.decoder,
    event1Decoder = d1.decoder,
    event1Encoder = JsonEncoderEvent1,
    event2Decoder = d2.decoder,
    event2Encoder = JsonEncoderEvent2,
    userDecoder = userJsonCodec.decoder,
    userEncoder = userJsonCodec.encoder
  )
}

object InMemoryZioJsonEventRepositorySpec extends ZIOSpecDefault {
  import ZioJsonCodecs._

  override val spec: Spec[TestEnvironment with Scope, Any] = suite("memory / zio-json")(
    EventRepositorySpec.spec(
      MemoryEventRepository.layer[JsonDecoder, JsonEncoder]
    )
  ) @@ TestAspect.timeout(1.minute)
}

object PostgresqlZioJsonEventRepositorySpec extends ZIOSpec[DbAdmin] {

  override val bootstrap: ULayer[DbAdmin with TestEnvironment] = testEnvironment ++ PostgresTestUtils.adminConnection

  import ZioJsonCodecs._

  override val spec: Spec[TestEnvironment with DbAdmin with Scope, Any] = suite("postgres / zio-json")(
    EventRepositorySpec.spec(
      ZLayer.makeSome[DbAdmin, EventRepository[JsonDecoder, JsonEncoder]](
        PostgresTestUtils.transactor,
        ZioJsonEventRepository.postgresLayer
      )
    )
  ) @@ TestAspect.timeout(2.minute) @@ TestAspect.parallelN(2)
}
