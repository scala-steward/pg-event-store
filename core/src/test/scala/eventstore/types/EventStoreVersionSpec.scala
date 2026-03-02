package eventstore.types

import zio.test.Assertion._
import zio.test.Spec
import zio.test.TestEnvironment
import zio.test.ZIOSpecDefault
import zio.test._

object EventStoreVersionSpec extends ZIOSpecDefault {

  override val spec: Spec[TestEnvironment, Any] = suite("EventStoreVersion")(
    suite("unsafe")(
      test("should be left with 'Illegal version number' when 0") {
        assert(EventStoreVersion.unsafe(0))(
          isLeft(equalTo("Illegal version number"))
        )
      },
      test("should be left with 'Illegal version number' when lower than initial version") {
        assert(EventStoreVersion.unsafe(EventStoreVersion.initial.asInt - 1))(
          isLeft(equalTo("Illegal version number"))
        )
      },
      test("should allow when initial version") {
        assert(EventStoreVersion.unsafe(EventStoreVersion.initial.asInt))(
          isRight(equalTo(EventStoreVersion.initial))
        )
      },
      test("should allow when greater than initial version") {
        val greater = EventStoreVersion.initial.next
        assert(EventStoreVersion.unsafe(greater.asInt))(
          isRight(equalTo(greater))
        )
      },
      test("should allow when Int.MaxValue") {
        assert(EventStoreVersion.unsafe(Int.MaxValue))(
          isRight(equalTo(EventStoreVersion(Int.MaxValue)))
        )
      }
    )
  )
}
