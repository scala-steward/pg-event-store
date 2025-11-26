package eventstore.ziojson


import eventstore.EventRepositorySpec.*
import eventstore.Scala3Spec
import eventstore.Scala3Spec.Union
import eventstore.memory.MemoryEventRepository
import eventstore.pg.test.PostgresTestUtils
import eventstore.pg.test.PostgresTestUtils.DbAdmin
import eventstore.ziojson.UnionCodec
import zio.Scope
import zio.ULayer
import zio.durationInt
import zio.json.*
import zio.test.*

private object Codecs {

  export ZioJsonCodecs.codecs

  import ZioJsonCodecs.*
  given JsonEncoder[Union] = {
    given JsonEncoder[(Event1, DoneBy1)] = JsonEncoder.tuple2[Event1, DoneBy1](JsonEncoderEvent1, doneBy1.encoder)
    given JsonEncoder[(Event2, DoneBy2)] = JsonEncoder.tuple2[Event2, DoneBy2](JsonEncoderEvent2, doneBy2.encoder)
    UnionCodec.sumTypeEncoder[(Event1, DoneBy1), (Event2, DoneBy2)]
  }
  given JsonDecoder[Union] = {
    given JsonDecoder[(Event1, DoneBy1)] = JsonDecoder.tuple2[Event1, DoneBy1](d1.decoder, doneBy1.decoder)
    given JsonDecoder[(Event2, DoneBy2)] = JsonDecoder.tuple2[Event2, DoneBy2](d2.decoder, doneBy2.decoder)
    UnionCodec.sumTypeDecoder[(Event1, DoneBy1), (Event2, DoneBy2)]
  }

}

object InMemoryZioJsonEventRepositoryScala3Spec extends ZIOSpecDefault {

  import Codecs.given

  override val spec: Spec[TestEnvironment with Scope, Any] = suite("memory / zio-json")(
    Scala3Spec.spec(MemoryEventRepository.layer[JsonDecoder, JsonEncoder])
  ) @@ TestAspect.timeout(1.minute)
}

object PostgresqlZioJsonEventRepositoryScala3Spec  extends ZIOSpec[DbAdmin] {

  import Codecs.given

  override val bootstrap: ULayer[DbAdmin with TestEnvironment] = testEnvironment ++ PostgresTestUtils.adminConnection

  override val spec: Spec[TestEnvironment with DbAdmin with Scope, Any] = suite("postgres / zio-json")(
    Scala3Spec.spec(PostgresqlZioJsonEventRepositorySpec.layer)
  ) @@ TestAspect.timeout(2.minute) @@ TestAspect.parallelN(2)


}
