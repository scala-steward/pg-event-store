package eventstore.memory

import eventstore.EventRepositorySpec
import eventstore.EventRepositorySpec.Codecs
import eventstore.memory.MemoryEventRepository.Id
import zio.Scope
import zio.durationInt
import zio.test._

object MemoryEventRepositorySpec extends ZIOSpecDefault {

  implicit val codecs: Codecs[Id, Id] = Codecs[Id, Id](
    eventDecoder = (),
    event1Decoder = (),
    event1Encoder = (),
    event2Decoder = (),
    event2Encoder = (),
    userDecoder = (),
    userEncoder = (),
    event1WithDoneBy1Encoder = (),
    event1WithDoneBy1Decoder = (),
    event2WithDoneBy2Encoder = (),
    event2WithDoneBy2Decoder = (),
    eventWithDoneByDecoder = (),
    eventWithUserEncoder = (),
    eventWithUserDecoder = ()
  )

  override val spec: Spec[TestEnvironment with Scope, Any] = suite("memory raw")(
    EventRepositorySpec.spec(MemoryEventRepository.layer[Id, Id])
  ) @@ TestAspect.timeout(2.minute) @@ TestAspect.parallelN(2)
}
