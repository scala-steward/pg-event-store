package eventstore

import eventstore.Generators.streamIdGen
import eventstore.RepositoryEventOps.Ops
import zio.Tag
import zio.test.Assertion._
import zio.test._

object RepositoryEventSpec extends ZIOSpecDefault {

  case class TypeA()
  case class TypeB()
  class SubTypeA() extends TypeA()

  def eventsGen[EventType: Tag](eventType: EventType): Gen[Any, RepositoryEvent[EventType]] =
    streamIdGen().mapZIO(streamId => eventType.asRepositoryEvent(streamId = streamId))

  def spec: Spec[TestEnvironment, Nothing] = suite("RepositoryEventSpec")(
    suite("isEventSubtypeOf")(
      test("should be false when types are different") {
        checkN(1)(eventsGen(TypeA())) { event =>
          assert(event.isEventSubtypeOf[TypeB])(isFalse)
        }
      },
      test("should be false when is a supertype") {
        checkN(1)(eventsGen(TypeA())) { event =>
          assert(event.isEventSubtypeOf[SubTypeA])(isFalse)
        }
      },
      test("should be true when types are the same") {
        checkN(1)(eventsGen(TypeA())) { event =>
          assert(event.isEventSubtypeOf[TypeA])(isTrue)
        }
      },
      test("should be true when is subtype") {
        checkN(1)(eventsGen(new SubTypeA())) { event =>
          assert(event.isEventSubtypeOf[TypeA])(isTrue)
        }
      }
    )
  )
}
