package eventstore

import eventstore.EventRepositorySpec.{A, Codecs, Event, Event1, Event2, eventsGen, event1Gen, doneBy1Gen, event2Gen, doneBy2Gen, DoneBy, DoneBy1, DoneBy2, Bob}
import eventstore.types.{AggregateId, AggregateName, EventStreamId, AggregateVersion}
import zio.test.*
import zio.test.Assertion.*
import zio.{TagK, URLayer, ZIO, durationInt}

object Scala3Spec {

  type Union = (Event1, DoneBy1) | (Event2, DoneBy2)

  def spec[R, Decoder[_]: TagK, Encoder[_]: TagK](
                                                   repository: URLayer[R, EventRepository[Decoder, Encoder]]
                                                 )(implicit codecs: Codecs[Decoder, Encoder], unionDecoder: Decoder[Union], unionEncoder: Encoder[Union]) = {

    given Decoder[(Event, DoneBy)] = codecs.eventWithDoneByDecoder
    given Decoder[(Event1, DoneBy1)] = codecs.event1WithDoneBy1Decoder
    given Decoder[(Event2, DoneBy2)] = codecs.event2WithDoneBy2Decoder
    given Encoder[(Event1, DoneBy1)] = codecs.event1WithDoneBy1Encoder
    given Encoder[(Event2, DoneBy2)] = codecs.event2WithDoneBy2Encoder

    suite("scala 3 examples")(
      test("should dispatch events to all listeners with union type listen") {
        ZIO.scoped {
          Live.live {
            for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              listener1 <- repository.listen[(Event1, DoneBy1)].map(_.events)
              listener2 <- repository.listen[Union].map(_.events)
              listener3 <- repository.listen[(Event, DoneBy)].map(_.events)
              listener4 <- repository.listen[(Event2, DoneBy2)].map(_.events)
              streamId <- AggregateId.generate.map(aggregateId => {
                EventStreamId(
                  aggregateId = aggregateId,
                  aggregateName = AggregateName("Foo")
                )
              })
              event <- (A: Event1).asRepositoryWriteEventWithDoneBy[DoneBy1](streamId = streamId, user = Bob)
              _ <- repository.saveEvents(streamId, Seq(event))
              events1 <- listener1.take(1).timeout(1.seconds).runCollect
              events2 <- listener2.take(1).timeout(1.seconds).runCollect
              events3 <- listener3.take(1).timeout(1.seconds).runCollect
              events4 <- listener4.take(1).timeout(1.seconds).runCollect
              expected <- (A: Event1).asRepositoryEvent(streamId = streamId)
            } yield assert(events1.asStrings)(hasSameElements(Seq(expected.asString))) &&
              assert(events2.asRepositoryWriteEvents.asStrings)(hasSameElements(Seq(expected.asString))) &&
              assert(events3.asStrings)(hasSameElements(Seq(expected.asString))) &&
              assert(events4.asStrings)(isEmpty)
          }
        }
      }.provideSome[R](repository),
      test("getEventStream should return appended events with user") {
          val generator =
            for (streamId, eventsA, _) <- eventsGen(event1Gen <*> doneBy1Gen, size1Gen = Gen.int(1, 20))
                version = eventsA.lastOption.map(_.aggregateVersion).getOrElse(AggregateVersion.initial)
                (_, eventsD, _) <- eventsGen(event2Gen <*> doneBy2Gen, fromVersion = version.next)
            yield (streamId, eventsA, eventsD.map(_.copy(aggregateId = streamId.aggregateId, aggregateName = streamId.aggregateName)))

          check(generator) { case (streamId, events1, events2) =>
            (ZIO.scoped (for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              _ <- repository.saveEvents[(Event1, DoneBy1)](streamId, events1)
              _ <- repository.saveEvents[(Event2, DoneBy2)](streamId, events2)
              stream <- repository.getEventStream[Union](streamId)
              result <- stream.runCollect
            } yield assert(result.asRepositoryWriteEvents)(equalTo(events1 ++ events2))))
              .provideSome[R](repository)
          }
        }


    ) @@ TestAspect.samples(10) @@ TestAspect.shrinks(0)
  }
}
