package eventstore.playjson

import eventstore.EventRepository
import eventstore.EventRepositorySpec
import eventstore.EventRepositorySpec.Codecs
import eventstore.EventRepositorySpec.DoneBy
import eventstore.EventRepositorySpec.DoneBy1
import eventstore.EventRepositorySpec.DoneBy2
import eventstore.EventRepositorySpec.Event
import eventstore.EventRepositorySpec.Event1
import eventstore.EventRepositorySpec.Event2
import eventstore.EventRepositorySpec.User
import eventstore.memory.MemoryEventRepository
import eventstore.pg.test.PostgresTestUtils
import eventstore.pg.test.PostgresTestUtils.DbAdmin
import julienrf.json.derived
import play.api.libs.json.Format
import play.api.libs.json.Json
import play.api.libs.json.Reads
import play.api.libs.json.Writes
import zio.Scope
import zio.ULayer
import zio.ZLayer
import zio.durationInt
import zio.test._

object PlayJsonCodecs {

  val eventCodec: Format[Event] = derived.oformat[Event]()
  val event1Codec: Format[Event1] = derived.oformat[Event1]()
  val event2Codec: Format[Event2] = derived.oformat[Event2]()

  val userJsonCodec: Format[User] = Json.format[User]

  val doneByJsonCodec: Format[DoneBy] = derived.oformat[DoneBy]()
  val doneBy1JsonCodec: Format[DoneBy1] = derived.oformat[DoneBy1]()
  val doneBy2JsonCodec: Format[DoneBy2] = derived.oformat[DoneBy2]()

  val eventWithDoneByCodec: Format[(Event, DoneBy)] = {
    implicit val event: Format[Event] = eventCodec
    implicit val doneBy: Format[DoneBy] = doneByJsonCodec
    derived.oformat[(Event, DoneBy)]()
  }

  val event1WithDoneBy1Codec: Format[(Event1, DoneBy1)] = {
    implicit val event1: Format[Event1] = event1Codec
    implicit val doneBy1: Format[DoneBy1] = doneBy1JsonCodec
    derived.oformat[(Event1, DoneBy1)]()
  }

  val event2WithDoneBy2Codec: Format[(Event2, DoneBy2)] = {
    implicit val event2: Format[Event2] = event2Codec
    implicit val doneBy2: Format[DoneBy2] = doneBy2JsonCodec
    derived.oformat[(Event2, DoneBy2)]()
  }

  val eventWithUserCodec: Format[(Event, User)] = {
    implicit val event1: Format[Event] = eventCodec
    implicit val user: Format[User] = userJsonCodec
    derived.oformat[(Event, User)]()
  }

  implicit val codecs: Codecs[Reads, Writes] = Codecs(
    eventDecoder = eventCodec,
    event1Decoder = event1Codec,
    event1Encoder = event1Codec,
    event2Decoder = event2Codec,
    event2Encoder = event2Codec,
    userDecoder = userJsonCodec,
    userEncoder = userJsonCodec,
    event1WithDoneBy1Encoder = event1WithDoneBy1Codec,
    event1WithDoneBy1Decoder = event1WithDoneBy1Codec,
    event2WithDoneBy2Encoder = event2WithDoneBy2Codec,
    event2WithDoneBy2Decoder = event2WithDoneBy2Codec,
    eventWithDoneByDecoder = eventWithDoneByCodec,
    eventWithUserEncoder = eventWithUserCodec,
    eventWithUserDecoder = eventWithUserCodec
  )

}

object MemoryPlayJsonEventRepositorySpec extends ZIOSpecDefault {

  import PlayJsonCodecs._

  override val spec: Spec[TestEnvironment with Scope, Any] = suite("memory / play-json (ignoring codecs)")(
    EventRepositorySpec.spec(MemoryEventRepository.layer[Reads, Writes])
  ) @@ TestAspect.timeout(1.minute)
}

object PostgresqlPlayJsonEventRepositorySpec extends ZIOSpec[DbAdmin] {

  override val bootstrap: ULayer[DbAdmin with TestEnvironment] = testEnvironment ++ PostgresTestUtils.adminConnection

  import PlayJsonCodecs._

  override val spec: Spec[TestEnvironment with DbAdmin with Scope, Any] = suite("postgres / play-json")(
    EventRepositorySpec.spec(
      ZLayer.makeSome[DbAdmin, EventRepository[Reads, Writes]](
        PostgresTestUtils.transactor,
        PlayJsonEventRepository.postgresLayer
      )
    )
  ) @@ TestAspect.timeout(2.minute) @@ TestAspect.parallelN(2)
}
