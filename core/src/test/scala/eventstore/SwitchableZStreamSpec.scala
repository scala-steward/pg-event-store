package eventstore

import eventstore.SwitchableZStream.Message
import zio.Chunk
import zio.Ref
import zio.Schedule
import zio.ZIO
import zio.durationInt
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

object SwitchableZStreamSpec extends ZIOSpecDefault {

  def spec = suite("SwitchableZStream")(
    test("should close past event streams") {
      for {
        counter <- Ref.make(0)
        pastEvents =
          for {
            count <- counter.updateAndGet(_ + 1)
            _ <- ZIO.fail("count should never be greater than 1").when(count > 1)
          } yield ZStream("past").ensuring(counter.update(_ - 1))
        sut <- SwitchableZStream.from(
          stream1 = ZStream.repeat("live"),
          stream2 = pastEvents
        )
        events <- sut.stream
          .collect { case Message.Event(v) => v }
          .tap {
            case "live" => sut.switchToPastEvents(until = _ == "past")
            case _      => ZIO.unit
          }
          .take(6)
          .runCollect
          .exit
      } yield assert(events)(succeeds(equalTo(Chunk("live", "past", "live", "past", "live", "past"))))
    },
    test("should die when reaching the end of past-events stream before the until predicate") {
      for {
        sut <- SwitchableZStream.from(
          stream1 = ZStream.repeat(0).schedule(Schedule.spaced(10.milliseconds)),
          stream2 = ZIO.succeed(ZStream(1))
        )
        fiber <- sut.stream.runDrain.fork

        _ <- TestClock.adjust(10.milliseconds)
        _ <- sut.switchToPastEvents(until = _ => false)
        _ <- TestClock.adjust(10.milliseconds)
        exit <- fiber.join.exit
      } yield assert(exit)(dies(anything))
    },
    test("should not fail when restarting without any event") {
      for {
        sut <- SwitchableZStream.from(
          stream1 = ZStream.repeat(0).schedule(Schedule.spaced(10.milliseconds)),
          stream2 = ZIO.succeed(ZStream.empty)
        )
        fiber <- sut.stream.take(2).runDrain.fork

        _ <- sut.switchToEmptyPastEvents
        _ <- TestClock.adjust(10.milliseconds)
        exit <- fiber.join.exit
      } yield assert(exit)(succeeds(anything))
    }
  )

}
