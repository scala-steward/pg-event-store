package eventstore.playjson

import eventstore.EventRepository
import eventstore.EventRepositorySpec
import eventstore.EventRepositorySpec.Codecs
import eventstore.EventRepositorySpec.Event
import eventstore.EventRepositorySpec.Event1
import eventstore.EventRepositorySpec.Event2
import eventstore.EventRepositorySpec.User
import eventstore.memory.MemoryEventRepository
import eventstore.pg.test.PostgresTestUtils
import eventstore.pg.test.PostgresTestUtils.DbAdmin
import eventstore.playjson.PlayJsonEventRepository
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

  val d1: Format[Event1] = derived.oformat[Event1]()

  val JsonEncoderEvent1: Writes[Event1] = derived.owrites[Event1]()

  val JsonEncoderEvent2: Writes[Event2] = derived.owrites[Event2]()

  val d2: Format[Event2] = derived.oformat[Event2]()

  val JsonReadsEvent: Reads[Event] =
    d1.asInstanceOf[Format[Event]].orElse(d2.asInstanceOf[Format[Event]])

  val userJsonCodec: Format[User] = Json.format[User]

  implicit val codecs: Codecs[Reads, Writes] = Codecs(
    eventDecoder = JsonReadsEvent,
    event1Decoder = d1,
    event1Encoder = JsonEncoderEvent1,
    event2Decoder = d2,
    event2Encoder = JsonEncoderEvent2,
    userDecoder = userJsonCodec,
    userEncoder = userJsonCodec
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
