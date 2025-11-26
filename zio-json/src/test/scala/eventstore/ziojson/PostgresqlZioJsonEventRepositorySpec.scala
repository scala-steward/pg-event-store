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

  val doneBy1: JsonCodec[DoneBy1] = DeriveJsonCodec.gen[DoneBy1]

  val JsonEncoderEvent1: JsonEncoder[Event1] = DeriveJsonEncoder.gen[Event1]

  val JsonEncoderEvent2: JsonEncoder[Event2] = DeriveJsonEncoder.gen[Event2]

  val d2: JsonCodec[Event2] = DeriveJsonCodec.gen[Event2]

  val doneBy2: JsonCodec[DoneBy2] = DeriveJsonCodec.gen[DoneBy2]

  val JsonCodecEvent: JsonCodec[Event] = DeriveJsonCodec.gen[Event]

  val doneBy: JsonCodec[DoneBy] = DeriveJsonCodec.gen[DoneBy]

  val userJsonCodec: JsonCodec[User] = DeriveJsonCodec.gen[User]
  val automaticProcessJsonCodec: JsonCodec[AutomaticProcess] = DeriveJsonCodec.gen[AutomaticProcess]

  val event1WithUserCodec: JsonCodec[(Event1, User)] = d1 <*> userJsonCodec

  val eventWithUserCodec: JsonCodec[(Event, User)] = JsonCodecEvent <*> userJsonCodec

  val eventWithDoneByCodec: JsonCodec[(Event, DoneBy)] = JsonCodecEvent <*> doneBy

  val event1WithDoneBy1Codec: JsonCodec[(Event1, DoneBy1)] = d1 <*> doneBy1

  val event2WithDoneBy2Codec: JsonCodec[(Event2, DoneBy2)] = d2 <*> doneBy2

  val event2WithAutomaticProcessCodec: JsonCodec[(Event2, AutomaticProcess)] = d2 <*> automaticProcessJsonCodec

  implicit val codecs: Codecs[JsonDecoder, JsonEncoder] = Codecs(
    eventDecoder = JsonCodecEvent.decoder,
    event1Decoder = d1.decoder,
    event1Encoder = JsonEncoderEvent1,
    event2Decoder = d2.decoder,
    event2Encoder = JsonEncoderEvent2,
    userDecoder = userJsonCodec.decoder,
    userEncoder = userJsonCodec.encoder,
    event1WithDoneBy1Encoder = event1WithDoneBy1Codec.encoder,
    event1WithDoneBy1Decoder = event1WithDoneBy1Codec.decoder,
    event2WithDoneBy2Encoder = event2WithDoneBy2Codec.encoder,
    event2WithDoneBy2Decoder = event2WithDoneBy2Codec.decoder,
    eventWithDoneByDecoder = eventWithDoneByCodec.decoder,
    eventWithUserEncoder = eventWithUserCodec.encoder,
    eventWithUserDecoder = eventWithUserCodec.decoder
  )

}

object InMemoryZioJsonEventRepositorySpec extends ZIOSpecDefault {
  import ZioJsonCodecs._

  override val spec: Spec[TestEnvironment with Scope, Any] = suite("memory / zio-json")(
    EventRepositorySpec.spec(MemoryEventRepository.layer[JsonDecoder, JsonEncoder])
  ) @@ TestAspect.timeout(1.minute)
}

object PostgresqlZioJsonEventRepositorySpec extends ZIOSpec[DbAdmin] {

  override val bootstrap: ULayer[DbAdmin with TestEnvironment] = testEnvironment ++ PostgresTestUtils.adminConnection

  import ZioJsonCodecs.codecs

  def layer: ZLayer[DbAdmin, Nothing, EventRepository[JsonDecoder, JsonEncoder]] =
    ZLayer.makeSome[DbAdmin, EventRepository[JsonDecoder, JsonEncoder]](
      PostgresTestUtils.transactor,
      ZioJsonEventRepository.postgresLayer
    )

  override val spec: Spec[TestEnvironment with DbAdmin with Scope, Any] = suite("postgres / zio-json")(
    EventRepositorySpec.spec(layer)
  ) @@ TestAspect.timeout(2.minute) @@ TestAspect.parallelN(2)
}
