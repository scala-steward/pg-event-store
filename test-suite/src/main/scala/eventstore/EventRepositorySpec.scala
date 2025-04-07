package eventstore

import eventstore.EventRepository.Direction.Backward
import eventstore.EventRepository.LastEventToHandle.LastEvent
import eventstore.EventRepository.LastEventToHandle.Version
import zio.Chunk
import zio.Random
import zio.Ref
import zio.Scope
import zio.Tag
import zio.TagK
import zio.URLayer
import zio.ZIO
import zio.durationInt
import zio.stream.UStream
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.test.magnolia.DeriveGen

import java.time.OffsetDateTime

import types.{AggregateId, AggregateName, AggregateVersion, EventStoreVersion, EventStreamId, ProcessId}
import EventRepository.Error.{Unexpected, VersionConflict}
import EventRepository.{Direction, SaveEventError, Subscription}

object EventRepositorySpec {
  case class User(id: String)
  case class AutomaticProcess(id: String)

  sealed trait DoneBy

  sealed trait DoneBy1 extends DoneBy
  case object Bob extends DoneBy1
  case object Alice extends DoneBy1

  sealed trait DoneBy2 extends DoneBy
  case object Pete extends DoneBy2
  case object Paul extends DoneBy2

  sealed trait Event

  sealed trait Event1 extends Event
  case object A extends Event1
  case class B(foo: Boolean, bar: Int) extends Event1

  sealed trait Event2 extends Event
  case object C extends Event2
  case class D(foo: Boolean, bar: Int) extends Event2

  implicit class WriteEventOps[+E](
      events: Seq[RepositoryEvent[E]]
  ) {
    def asRepositoryWriteEvents =
      events.map(event =>
        RepositoryWriteEvent(
          processId = event.processId,
          aggregateId = event.aggregateId,
          aggregateName = event.aggregateName,
          aggregateVersion = event.aggregateVersion,
          sentDate = event.sentDate,
          event = event.event
        )
      )
  }

  implicit class Ops[A: Tag](a: A) {
    def asRepositoryEvent(
        version: AggregateVersion = AggregateVersion.initial,
        eventStoreVersion: EventStoreVersion = EventStoreVersion.initial,
        streamId: EventStreamId
    ) = {
      for {
        processId <- ProcessId.generate
      } yield RepositoryEvent[A](
        processId = processId,
        aggregateId = streamId.aggregateId,
        aggregateName = streamId.aggregateName,
        sentDate = OffsetDateTime.parse("2027-12-03T10:15:30+01:00"),
        aggregateVersion = version,
        event = a,
        eventStoreVersion = eventStoreVersion
      )
    }
    def asRepositoryWriteEvent(
        version: AggregateVersion = AggregateVersion.initial,
        streamId: EventStreamId
    ) = {
      for {
        processId <- ProcessId.generate
      } yield RepositoryWriteEvent[A](
        processId = processId,
        aggregateId = streamId.aggregateId,
        aggregateName = streamId.aggregateName,
        sentDate = OffsetDateTime.parse("2027-12-03T10:15:30+01:00"),
        aggregateVersion = version,
        event = a
      )
    }
    def asRepositoryWriteEventWithDoneBy[U](
        version: AggregateVersion = AggregateVersion.initial,
        streamId: EventStreamId,
        user: U
    ) = {
      for {
        processId <- ProcessId.generate
      } yield RepositoryWriteEvent[(A, U)](
        processId = processId,
        aggregateId = streamId.aggregateId,
        aggregateName = streamId.aggregateName,
        sentDate = OffsetDateTime.parse("2027-12-03T10:15:30+01:00"),
        aggregateVersion = version,
        event = (a, user)
      )
    }
  }

  val versionGen: Gen[Any, AggregateVersion] = {
    Gen
      .int(0, 20)
      .map(version => 0.to(version).foldLeft(AggregateVersion.initial)((v, _) => v.next))
  }
  val aggregateIdGen: Gen[Any, AggregateId] =
    Gen.uuid.map(id => AggregateId(id))
  val aggregateNameGen: Gen[Any, AggregateName] =
    Gen.alphaNumericString.map(name => AggregateName(name))
  def streamIdGen(
      nameGen: Gen[Any, AggregateName] = aggregateNameGen
  ): Gen[Any, EventStreamId] = {
    aggregateIdGen.zip(nameGen).map { case (id, name) =>
      EventStreamId(id, name)
    }
  }
  val event1Gen: Gen[Any, Event1] = DeriveGen.gen[Event1].derive
  val event2Gen: Gen[Any, Event2] = DeriveGen.gen[Event2].derive
  val eventGen: Gen[Any, Event] = DeriveGen.gen[Event].derive
  val userGen: Gen[Any, User] = Gen.alphaNumericString.filterNot(_.isEmpty).map(User.apply)
  val doneBy1Gen: Gen[Any, DoneBy1] = DeriveGen.gen[DoneBy1].derive
  val doneBy2Gen: Gen[Any, DoneBy2] = DeriveGen.gen[DoneBy2].derive

  def pickRandomly[A](lists: List[List[A]]): UStream[A] = {
    ZStream.unfoldZIO(lists)(lists => {
      if (lists.isEmpty) ZIO.none
      else {
        for {
          listNumber <- Random.nextIntBetween(0, lists.length)
          head :: tail = lists(listNumber): @unchecked
          updatedLists = lists.updated(listNumber, tail).filter(_.nonEmpty)
        } yield Some((head, updatedLists))
      }
    })
  }

  def eventsGen[EventType: Tag](
      eventGen: Gen[Any, EventType],
      nameGen: Gen[Any, AggregateName] = aggregateNameGen,
      size1Gen: Gen[Any, Int] = Gen.int(0, 20),
      size2Gen: Gen[Any, Int] = Gen.int(0, 20),
      fromVersion: AggregateVersion = AggregateVersion.initial
  ): Gen[
    Any,
    (
        EventStreamId,
        List[RepositoryWriteEvent[EventType]],
        List[RepositoryWriteEvent[EventType]]
    )
  ] = for {
    streamId <- streamIdGen(nameGen)
    size1 <- size1Gen
    size2 <- size2Gen
    events <- genEventSeq(fromVersion, size1 + size2, streamId, eventGen)
    (events1, events2) = events.splitAt(size1)
  } yield (streamId, events1, events2)

  def eventsGen3[EventType: Tag](
      eventGen: Gen[Any, EventType],
      nameGen: Gen[Any, AggregateName] = aggregateNameGen,
      size1Gen: Gen[Any, Int] = Gen.int(0, 20),
      size2Gen: Gen[Any, Int] = Gen.int(0, 20),
      size3Gen: Gen[Any, Int] = Gen.int(0, 20),
      fromVersion: AggregateVersion = AggregateVersion.initial
  ): Gen[
    Any,
    (
        EventStreamId,
        List[RepositoryWriteEvent[EventType]],
        List[RepositoryWriteEvent[EventType]],
        List[RepositoryWriteEvent[EventType]]
    )
  ] = for {
    streamId <- streamIdGen(nameGen)
    size1 <- size1Gen
    size2 <- size2Gen
    size3 <- size3Gen
    events <- genEventSeq(fromVersion, size1 + size2 + size3, streamId, eventGen)
    (events1, remainder) = events.splitAt(size1)
    (events2, events3) = remainder.splitAt(size2)
  } yield (streamId, events1, events2, events3)

  private def genEventSeq[EventType: Tag](
      fromVersion: AggregateVersion,
      size: Int,
      streamId: EventStreamId,
      eventGen: Gen[Any, EventType]
  ) = {
    Gen.unfoldGenN(size)(fromVersion)(version => {
      eventGen.mapZIO(event => {
        event
          .asRepositoryWriteEvent(version = version, streamId = streamId)
          .map(event => version.next -> event)
      })
    })
  }

  val atLeastOne = Gen.int(1, 20)

  case class Codecs[Decoder[_], Encoder[_]](
      eventDecoder: Decoder[Event],
      event1Decoder: Decoder[Event1],
      event1Encoder: Encoder[Event1],
      event2Decoder: Decoder[Event2],
      event2Encoder: Encoder[Event2],
      userDecoder: Decoder[User],
      userEncoder: Encoder[User],
      event1WithDoneBy1Encoder: Encoder[(Event1, DoneBy1)],
      event1WithDoneBy1Decoder: Decoder[(Event1, DoneBy1)],
      event2WithDoneBy2Encoder: Encoder[(Event2, DoneBy2)],
      event2WithDoneBy2Decoder: Decoder[(Event2, DoneBy2)],
      eventWithDoneByDecoder: Decoder[(Event, DoneBy)],
      eventWithUserEncoder: Encoder[(Event, User)],
      eventWithUserDecoder: Decoder[(Event, User)]
  )

  def spec[R, Decoder[_]: TagK, Encoder[_]: TagK](
      repository: URLayer[R, EventRepository[Decoder, Encoder]]
  )(implicit codecs: Codecs[Decoder, Encoder]): Spec[R with Scope, Any] = {
    suite("EventRepository spec")(
      suite("examples")(
        test("should not fail when saving a single event") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder
          for {
            firstStreamId <- AggregateId.generate.map(aggregateId => {
              EventStreamId(
                aggregateId = aggregateId,
                aggregateName = AggregateName("Foo")
              )
            })
            event <- (A: Event1).asRepositoryWriteEvent(streamId = firstStreamId)
            _ <- ZIO.serviceWithZIO[EventRepository[Decoder, Encoder]](
              _.saveEvents(firstStreamId, Seq(event))
            )
          } yield assertCompletes
        },
        test("should fail when first event has Version > Version.initial") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder
          for {
            firstStreamId <- AggregateId.generate.map(aggregateId => {
              EventStreamId(
                aggregateId = aggregateId,
                aggregateName = AggregateName("Foo")
              )
            })
            event <-
              (A: Event1).asRepositoryWriteEvent(
                streamId = firstStreamId,
                version = AggregateVersion.initial.next
              )
            result <- ZIO
              .serviceWithZIO[EventRepository[Decoder, Encoder]](
                _.saveEvents(firstStreamId, Seq(event))
              )
              .either
          } yield assert(result)(
            isLeft(
              equalTo(
                VersionConflict(
                  provided = AggregateVersion.initial.next,
                  required = AggregateVersion.initial
                )
              )
            )
          )
        },
        test("should not fail when empty event list") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder
          for {
            firstStreamId <- AggregateId.generate.map(aggregateId => {
              EventStreamId(
                aggregateId = aggregateId,
                aggregateName = AggregateName("Foo")
              )
            })
            _ <- ZIO.serviceWithZIO[EventRepository[Decoder, Encoder]](
              _.saveEvents[Event1](firstStreamId, Seq.empty)
            )
          } yield assertCompletes
        },
        test(
          "getEventStream should return empty seq when eventStreamId doesn't exist"
        ) {
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder
          for {
            firstStreamId <- AggregateId.generate.map(aggregateId => {
              EventStreamId(
                aggregateId = aggregateId,
                aggregateName = AggregateName("Foo")
              )
            })
            stream <- ZIO.serviceWithZIO[EventRepository[Decoder, Encoder]](
              _.getEventStream[Event1](firstStreamId)
            )
            result <- stream.runCollect
          } yield assert(result)(isEmpty)
        },
        test("should dispatch events to all listeners") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder
          ZIO.scoped {
            for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              listener1 <- repository.listen[Event1].map(_.events)
              listener2 <- repository.listen[Event1].map(_.events)
              streamId <- AggregateId.generate.map(aggregateId => {
                EventStreamId(
                  aggregateId = aggregateId,
                  aggregateName = AggregateName("Foo")
                )
              })
              event <- (A: Event1).asRepositoryWriteEvent(streamId = streamId)
              events = Seq(event)
              _ <- repository.saveEvents(streamId, events)
              events1 <- listener1.take(1).timeout(1.seconds).runCollect
              events2 <- listener2.take(1).timeout(1.seconds).runCollect
              expected <- (A: Event1).asRepositoryEvent(streamId = streamId)
            } yield assert(events1.asStrings)(
              hasSameElements(Seq(expected.asString))
            ) &&
              assert(events2.asRepositoryWriteEvents.asStrings)(
                hasSameElements(Seq(expected.asString))
              )
          }
        },
        test("listEventStreamWithName should return an empty stream") {
          ZIO.scoped {
            for {
              actual <- ZIO.serviceWithZIO[EventRepository[Decoder, Encoder]](
                _.listEventStreamWithName(AggregateName("Foo")).runCollect
              )
            } yield assert(actual)(isEmpty)
          }
        }
      ).provideSome[R & Scope](repository),
      suite("examples with event + doneBy")(
        test("should dispatch events to all listeners") {
          implicit val implicitEvent1WithDoneBy1Encoder: Encoder[(Event1, DoneBy1)] = codecs.event1WithDoneBy1Encoder
          implicit val implicitEvent1WithDoneBy1Decoder: Decoder[(Event1, DoneBy1)] = codecs.event1WithDoneBy1Decoder
          implicit val implicitEvent2WithDoneBy2Decoder: Decoder[(Event2, DoneBy2)] = codecs.event2WithDoneBy2Decoder
          implicit val implicitEventWithDoneByDecoder: Decoder[(Event, DoneBy)] = codecs.eventWithDoneByDecoder
          ZIO.scoped {
            Live.live {
              for {
                repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                listener1 <- repository.listen[(Event1, DoneBy1)].map(_.events)
                listener2 <- repository.listen[(Event2, DoneBy2)].map(_.events)
                listener3 <- repository.listen[(Event, DoneBy)].map(_.events)
                streamId <- AggregateId.generate.map(aggregateId => {
                  EventStreamId(
                    aggregateId = aggregateId,
                    aggregateName = AggregateName("Foo")
                  )
                })
                event <- (A: Event1).asRepositoryWriteEventWithDoneBy[DoneBy1](streamId = streamId, user = Bob)
                events = Seq(event)
                _ <- repository.saveEvents(streamId, events)
                events1 <- listener1.take(1).timeout(1.seconds).runCollect
                events2 <- listener2.take(1).timeout(1.seconds).runCollect
                events3 <- listener3.take(1).timeout(1.seconds).runCollect
                expected <- (A: Event1).asRepositoryEvent(streamId = streamId)
              } yield assert(events1.asStrings)(hasSameElements(Seq(expected.asString))) &&
                assert(events2.asRepositoryWriteEvents.asStrings)(isEmpty) &&
                assert(events3.asStrings)(hasSameElements(Seq(expected.asString)))
            }
          }
        }.provideSome[R](repository)
      ),
      suite("properties")(
        test("should fail when aggregateVersion is not previous.next") {

          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(versionGen.filterNot(_ == AggregateVersion.initial.next)) { v =>
            (for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              streamId <- AggregateId.generate.map(aggregateId => {
                EventStreamId(
                  aggregateId = aggregateId,
                  aggregateName = AggregateName("Foo")
                )
              })
              event <- A.asRepositoryWriteEvent(streamId = streamId)
              savedEvents <- repository.saveEvents[Event1](streamId, Seq(event))
              secondEvent <- A.asRepositoryWriteEvent(version = v, streamId = streamId)
              error <- repository.saveEvents[Event1](streamId, Seq(secondEvent)).either
            } yield assert(error)(
              isLeft(
                equalTo(
                  VersionConflict(
                    provided = v,
                    required = savedEvents.last.aggregateVersion.next
                  )
                )
              )
            )).provideSome[R](repository)
          }
        },
        test(
          "should fail when passed events are not a series of following increments"
        ) {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(versionGen.zip(versionGen).filterNot { case (v1, v2) =>
            v1.next == v2
          }) { case (v1, v2) =>
            (for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              streamId <- AggregateId.generate.map { aggregateId =>
                EventStreamId(
                  aggregateId = aggregateId,
                  aggregateName = AggregateName("Foo")
                )
              }

              events <- ZIO.collectAll(
                Seq(
                  A.asRepositoryWriteEvent(streamId = streamId, version = AggregateVersion.initial),
                  A.asRepositoryWriteEvent(streamId = streamId, version = v1),
                  A.asRepositoryWriteEvent(streamId = streamId, version = v2)
                )
              )
              error <- repository.saveEvents[Event1](streamId, events).either
            } yield assert(error)(
              isLeft(
                isSubtype[Unexpected](
                  hasField(
                    "throwable",
                    _.throwable,
                    isSubtype[IllegalArgumentException](
                      hasMessage(containsString("Invalid version sequence"))
                    )
                  )
                )
              )
            )).provideSome[R](repository)
          }
        },
        test("getEventStream should return appended events") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen)) { case (firstStreamId, events, _) =>
            (for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              _ <- repository.saveEvents(firstStreamId, events)
              stream <- repository.getEventStream[Event1](firstStreamId)
              result <- stream.runCollect
            } yield assert(result.asRepositoryWriteEvents)(equalTo(events)))
              .provideSome[R & Scope](repository)
          }
        },
        test("getEventStream should return appended events in reverse order") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen)) { case (firstStreamId, events, _) =>
            (for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              _ <- repository.saveEvents(firstStreamId, events)
              stream <- repository.getEventStream[Event1](firstStreamId, direction = Backward)
              result <- stream.runCollect
            } yield assert(result.asRepositoryWriteEvents)(equalTo(events.reverse)))
              .provideSome[R & Scope](repository)
          }
        },
        test("getEventStream should return appended events with doneby") {

          implicit val implicitEventWithUserDecoder: Decoder[(Event, User)] = codecs.eventWithUserDecoder
          implicit val implicitEventWithUserEncoder: Encoder[(Event, User)] = codecs.eventWithUserEncoder

          check(eventsGen(eventGen <*> userGen)) { case (firstStreamId, events, _) =>
            ZIO
              .scoped(for {
                repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                _ <- repository.saveEvents(firstStreamId, events)
                stream <- repository.getEventStream[(Event, User)](firstStreamId)
                result <- stream.runCollect
              } yield assert(result.asRepositoryWriteEvents)(equalTo(events)))
              .provideSome[R](repository)
          }
        },
        test("listen should stream appended events") {

          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen)) { case (firstStreamId, events, _) =>
            ZIO
              .scoped(for {
                repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                stream <- repository.listen[Event1].map(_.events)
                _ <- repository.saveEvents(firstStreamId, events)
                result <- stream
                  .take(events.length.toLong)
                  .timeout(1.seconds)
                  .runCollect
              } yield assert(result.toList.asRepositoryWriteEvents)(equalTo(events)))
              .provideSome[R](repository)
          }
        },
        test("listen should stream appended events with doneby") {
          implicit val implicitEventWithUserDecoder: Decoder[(Event, User)] = codecs.eventWithUserDecoder
          implicit val implicitEventWithUserEncoder: Encoder[(Event, User)] = codecs.eventWithUserEncoder

          check(eventsGen(eventGen <*> userGen)) { case (firstStreamId, events, _) =>
            ZIO
              .scoped(for {
                repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                stream <- repository.listen[(Event, User)].map(_.events)
                _ <- repository.saveEvents(firstStreamId, events)
                result <- stream
                  .take(events.length.toLong)
                  .timeout(1.seconds)
                  .runCollect
              } yield assert(result.toList.asRepositoryWriteEvents)(equalTo(events)))
              .provideSome[R](repository)
          }
        },
        test("listen should stream only selected types") {

          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder
          implicit val implicitEvent2Encoder: Encoder[Event2] = codecs.event2Encoder
          implicit val implicitEvent2Decoder: Decoder[Event2] = codecs.event2Decoder

          check(eventsGen(event1Gen), eventsGen(event2Gen)) {
            case ((secondStreamId, events1, _), (firstStreamId, events2, _)) =>
              ZIO
                .scoped(
                  for {
                    repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                    stream <- repository.listen[Event1].map(_.events)
                    _ <- repository.saveEvents(firstStreamId, events2)
                    _ <- repository.saveEvents(secondStreamId, events1)
                    result <- stream
                      .take(events1.length.toLong)
                      .timeout(1.seconds)
                      .runCollect
                  } yield assert(result.toList.asRepositoryWriteEvents)(
                    equalTo(events1)
                  )
                )
                .provideSome[R](repository)
          }
        },
        test("listen should stream only selected types with tuple") {

          implicit val implicitEvent1WithDoneBy1Encoder: Encoder[(Event1, DoneBy1)] = codecs.event1WithDoneBy1Encoder
          implicit val implicitEvent1WithDoneBy1Decoder: Decoder[(Event1, DoneBy1)] = codecs.event1WithDoneBy1Decoder
          implicit val implicitEvent2WithDoneBy2Encoder: Encoder[(Event2, DoneBy2)] = codecs.event2WithDoneBy2Encoder
          implicit val implicitEvent2WithDoneBy2Decoder: Decoder[(Event2, DoneBy2)] = codecs.event2WithDoneBy2Decoder

          check(eventsGen(event1Gen <*> doneBy1Gen), eventsGen(event2Gen <*> doneBy2Gen)) {
            case ((secondStreamId, events, _), (firstStreamId, events2, _)) =>
              ZIO
                .scoped(
                  for {
                    repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                    stream <- repository.listen[(Event2, DoneBy2)].map(_.events)
                    _ <- repository.saveEvents(firstStreamId, events2)
                    _ <- repository.saveEvents(secondStreamId, events)
                    result <- stream
                      .take(events2.length.toLong)
                      .timeout(1.seconds)
                      .runCollect
                  } yield assert(result.toList.asRepositoryWriteEvents)(equalTo(events2))
                )
                .provideSome[R](repository)
          }
        }, {
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent2Decoder: Decoder[Event2] = codecs.event2Decoder
          implicit val implicitEvent2Encoder: Encoder[Event2] = codecs.event2Encoder
          implicit val implicitEventDecoder: Decoder[Event] = codecs.eventDecoder

          def save(repositoryEvent: RepositoryWriteEvent[Any]) = {
            import repositoryEvent._
            def save[EE: Decoder: Encoder: Tag](event: EE) = {
              ZIO.serviceWithZIO[EventRepository[Decoder, Encoder]](
                _.saveEvents[EE](
                  EventStreamId(aggregateId, aggregateName),
                  List(
                    RepositoryWriteEvent[EE](
                      processId = processId,
                      aggregateId = aggregateId,
                      aggregateName = aggregateName,
                      aggregateVersion = aggregateVersion,
                      sentDate = sentDate,
                      event = event
                    )
                  )
                )
              )
            }

            repositoryEvent.event match {
              case e: Event1 => save[Event1](e)
              case e: Event2 => save[Event2](e)
              case _         => ZIO.fail("should not happen")
            }
          }

          suite("getAllEvents")(
            test("getAllEvents should list events in a deterministic order") {

              check(eventsGen(event1Gen, size1Gen = atLeastOne), eventsGen(event2Gen, size1Gen = atLeastOne)) {
                case ((_, events1, _), (_, events2, _)) =>
                  ZIO
                    .scoped {
                      for {
                        repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                        events <- pickRandomly(List(events1, events2)).runCollect
                        _ <- ZIO.foreachDiscard(events)(save)
                        result <- repository.getAllEvents[Event].flatMap(_.runCollect)

                      } yield assert(result.asRepositoryWriteEvents)(
                        equalTo(events)
                      )
                    }
                    .provideSome[R](repository)
              }
            },
            test(
              "getAllEvents should list events in the same ordering than listen"
            ) {
              implicit val implicitEventDecoder: Decoder[Event] = codecs.eventDecoder

              check(eventsGen(event1Gen, size1Gen = atLeastOne), eventsGen(event2Gen, size1Gen = atLeastOne)) {
                case ((_, events1, _), (_, events2, _)) =>
                  ZIO
                    .scoped {
                      for {
                        repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                        stream <- repository.listen[Event].map(_.events)
                        _ <- pickRandomly(List(events1, events2)).runForeach(save)
                        fromListen <- stream
                          .take(events1.length.toLong + events2.length)
                          .runCollect
                        result <- repository
                          .getAllEvents[Event]
                          .flatMap(_.runCollect)

                      } yield assert(result)(equalTo(fromListen))
                    }
                    .provideSome[R](repository)
              }
            }
          )
        },
        test(
          "listEventStreamWithName should return the stream created when name match"
        ) {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen, size1Gen = atLeastOne)) { case (firstStreamId, events, _) =>
            (for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              _ <- repository.saveEvents(firstStreamId, events)
              actual <- repository
                .listEventStreamWithName(firstStreamId.aggregateName)
                .runCollect
            } yield assert(actual)(equalTo(Chunk(firstStreamId))))
              .provideSome[R](repository)
          }
        },
        test(
          "listEventStreamWithName should return empty stream when name doesn't match"
        ) {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen).flatMap { case a @ (streamId, _, _) =>
            Gen
              .const(a)
              .zip(aggregateNameGen.filter(_ != streamId.aggregateName))
          }) { case (firstStreamId, events, _, otherAggregateName) =>
            (for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              _ <- repository.saveEvents(firstStreamId, events)
              actual <- repository
                .listEventStreamWithName(otherAggregateName)
                .runCollect
            } yield assert(actual)(isEmpty)).provideSome[R](repository)
          }
        },
        test("listEventStreamWithName should only matching streams") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(
            eventsGen(event1Gen, Gen.const(AggregateName("Foo")), size1Gen = atLeastOne),
            eventsGen(event1Gen, Gen.const(AggregateName("Foo")), size1Gen = atLeastOne),
            eventsGen(
              event1Gen,
              aggregateNameGen.filter(_ != AggregateName("Foo")),
              size1Gen = atLeastOne
            )
          ) {
            case (
                  (stream1, events1, _),
                  (stream2, events2, _),
                  (stream3, events3, _)
                ) =>
              (for {
                repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                _ <- repository.saveEvents(stream1, events1)
                _ <- repository.saveEvents(stream2, events2)
                _ <- repository.saveEvents(stream3, events3)
                actual <- repository
                  .listEventStreamWithName(AggregateName("Foo"))
                  .runCollect
              } yield assert(actual)(hasSameElements(Seq(stream1, stream2))))
                .provideSome[R](repository)
          }
        },
        test("listEventStreamWithName should return streams by order of creation") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder
          check(
            Gen
              .setOfN(2)(aggregateNameGen)
              .flatMap(name =>
                Gen.listOfBounded(1, 20)(eventsGen(event1Gen, Gen.elements(name.toList*), size1Gen = atLeastOne))
              )
          ) { events =>
            (for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              firstId = events.head._1
              ids <- ZIO.foreach(events) { case (id, firstEvents, _) =>
                repository.saveEvents(id, firstEvents).as(id)
              }
              _ <- ZIO.foreachParDiscard(events) { case (id, _, remainder) =>
                repository.saveEvents(id, remainder)
              }
              actual <- repository
                .listEventStreamWithName(firstId.aggregateName)
                .runCollect
            } yield assert(actual.toList)(equalTo(ids.filter(_.aggregateName == firstId.aggregateName))))
              .provideSome[R](repository)
          }
        },
        test("listEventStreamWithName should return streams by reverse order of creation") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder
          check(
            Gen
              .setOfN(2)(aggregateNameGen)
              .flatMap(name =>
                Gen.listOfBounded(1, 20)(eventsGen(event1Gen, Gen.elements(name.toList*), size1Gen = atLeastOne))
              )
          ) { events =>
            (for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              firstId = events.head._1
              ids <- ZIO.foreach(events) { case (id, firstEvents, _) =>
                repository.saveEvents(id, firstEvents).as(id)
              }
              _ <- ZIO.foreachParDiscard(events) { case (id, _, remainder) =>
                repository.saveEvents(id, remainder)
              }
              actual <- repository
                .listEventStreamWithName(firstId.aggregateName, Direction.Backward)
                .runCollect
            } yield assert(actual.toList)(equalTo(ids.filter(_.aggregateName == firstId.aggregateName).reverse)))
              .provideSome[R](repository)
          }
        },
        test("save should not write events with conflicting versions") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen, size2Gen = atLeastOne)) { case (streamId, events1, events2) =>
            ZIO
              .scoped {
                val save = ZIO.serviceWithZIO[EventRepository[Decoder, Encoder]](_.saveEvents(streamId, events2).either)
                for {
                  _ <- ZIO.serviceWithZIO[EventRepository[Decoder, Encoder]](_.saveEvents(streamId, events1))
                  result <- ZIO.collectAllPar(ZIO.replicate(10)(save))
                  (success, failures) = result.partition(_.isRight)
                } yield {
                  assert(success)(hasSize(equalTo(1))) &&
                  assert(failures)(
                    forall(
                      isLeft[SaveEventError](
                        equalTo(
                          VersionConflict(
                            provided = events1.foldLeft(AggregateVersion.initial)((s, _) => s.next),
                            required = (events1 ++ events2).foldLeft(AggregateVersion.initial)((s, _) => s.next)
                          )
                        )
                      )
                    ) &&
                      hasSize(equalTo(9))
                  )
                }
              }
              .provideSome[R](repository)
          }
        }
      ),
      suite("listenFromVersion Spec - for projections keeping track of events offsets")(
        test("should stream past events from offset") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen, size1Gen = atLeastOne)) { case (firstStreamId, events1, events2) =>
            val nbEvents2 = events2.length.toLong
            ZIO
              .scoped(
                for {
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  fromVersion <- repository.saveEvents(firstStreamId, events1).map(_.last.eventStoreVersion)
                  _ <- repository.saveEvents(firstStreamId, events2)
                  subscription <- repository.listenFromVersion[Event1](fromExclusive = fromVersion)
                  result <- subscription.stream
                    .collect {
                      case e: RepositoryEvent[Event1] => e.asString
                      case _: Reset[?]                => "reset"
                    }
                    .take(nbEvents2)
                    .timeout(1.seconds)
                    .runCollect
                } yield assert(result.toList)(equalTo(events2.asStrings))
              )
              .provideSome[R](repository)
          }
        },
        test("should switch to first events") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen3(event1Gen, size2Gen = atLeastOne)) { case (firstStreamId, events1, events2, events3) =>
            val nbEvents1 = events1.length.toLong
            val nbEvents2 = events2.length.toLong
            val nbEvents3 = events3.length.toLong
            ZIO
              .scoped(
                for {
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  fromVersion <- repository.saveEvents(firstStreamId, events1).map(_.last.eventStoreVersion)
                  _ <- repository.saveEvents(firstStreamId, events2)
                  subscription <- repository.listenFromVersion[Event1](fromExclusive = fromVersion)
                  _ <- repository.saveEvents(firstStreamId, events3)
                  lastKnownVersionForEvents2 <- repository
                    .getAllEvents[Event1]
                    .flatMap(_.runLast)
                    .map(_.map(_.eventStoreVersion).getOrElse(EventStoreVersion.initial))

                  result <- subscription.stream
                    .collect { case e: RepositoryEvent[Event1] => e }
                    .take(nbEvents1 + nbEvents2 * 2 + nbEvents3 * 2)
                    .tap(event =>
                      ZIO.when(event.eventStoreVersion == lastKnownVersionForEvents2)(
                        subscription.restartFromFirstEvent(Version(lastKnownVersionForEvents2))
                      )
                    )
                    .timeout(1.seconds)
                    .runCollect
                } yield assert(result.toList.asRepositoryWriteEvents)(
                  equalTo(events2 ++ events3 ++ events1 ++ events2 ++ events3)
                )
              )
              .provideSome[R](repository)
          }
        }
      ),
      suite(
        "listen Spec - switch from live stream and getAll - For projections rebuild"
      )(
        test(
          "listen should publish reset event before feeding events from the past"
        ) {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen, size1Gen = atLeastOne)) { case (firstStreamId, events1, events2) =>
            val nbEvents1 = events1.length.toLong
            val nbEvents2 = events2.length.toLong
            ZIO
              .scoped(
                for {
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  _ <- repository.saveEvents(firstStreamId, events1)
                  subscription <- repository.listen[Event1]
                  _ <- repository.saveEvents(firstStreamId, events2)
                  lastKnownVersionForEvents2 <- repository
                    .getAllEvents[Event1]
                    .flatMap(_.runLast)
                    .map(
                      _.map(_.eventStoreVersion)
                        .getOrElse(EventStoreVersion.initial)
                    )
                  result <- subscription.stream
                    .tap {
                      case e: RepositoryEvent[Event1] =>
                        ZIO.when(
                          e.eventStoreVersion == lastKnownVersionForEvents2
                        )(
                          subscription.restartFromFirstEvent(
                            Version(lastKnownVersionForEvents2)
                          )
                        )
                      case _ => ZIO.unit
                    }
                    .collect {
                      case e: RepositoryEvent[Event1] => e.asString
                      case _: Reset[?]                => "reset"
                    }
                    .take(nbEvents1 + nbEvents2 * 2 + 1)
                    .timeout(1.seconds)
                    .runCollect
                } yield assert(result.toList)(
                  equalTo(
                    events2.asStrings ++ Seq(
                      "reset"
                    ) ++ events1.asStrings ++ events2.asStrings
                  )
                )
              )
              .provideSome[R](repository)
          }
        },
        test(
          "listen should publish reset event when we have events before and then reseting up to the last event"
        ) {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen, size2Gen = atLeastOne)) { case (firstStreamId, events1, events2) =>
            val nbEvents1 = events1.length.toLong
            val nbEvents2 = events2.length.toLong
            ZIO
              .scoped(
                for {
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  _ <- repository.saveEvents(firstStreamId, events1)
                  subscription <- repository.listen[Event1]
                  _ <- repository.saveEvents(firstStreamId, events2)
                  lastKnownVersionForEvents2 <- repository
                    .getAllEvents[Event1]
                    .flatMap(_.runLast)
                    .map(
                      _.map(_.eventStoreVersion)
                        .getOrElse(EventStoreVersion.initial)
                    )
                  result <- subscription.stream
                    .tap {
                      case e: RepositoryEvent[Event1] =>
                        ZIO.when(
                          e.eventStoreVersion == lastKnownVersionForEvents2
                        )(
                          subscription.restartFromFirstEvent(LastEvent)
                        )
                      case _ => ZIO.unit
                    }
                    .collect {
                      case e: RepositoryEvent[Event1] => e.asString
                      case _: Reset[?]                => "reset"
                    }
                    .take(nbEvents1 + nbEvents2 * 2 + 1)
                    .timeout(1.seconds)
                    .runCollect
                } yield assert(result.toList)(
                  equalTo(
                    events2.asStrings ++ Seq(
                      "reset"
                    ) ++ events1.asStrings ++ events2.asStrings
                  )
                )
              )
              .provideSome[R](repository)
          }
        },
        test(
          "listen should publish reset event when reseting up to the last event but no events yet"
        ) {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen, size1Gen = atLeastOne)) { case (firstStreamId, events1, _) =>
            ZIO
              .scoped(
                for {
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  subscription <- repository.listen[Event1]
                  resultFiber <- Live.live(
                    subscription.stream
                      .tap {
                        case _: Reset[?] => repository.saveEvents(firstStreamId, events1)
                        case _           => ZIO.unit
                      }
                      .collect {
                        case e: RepositoryEvent[Event1] => e.asString
                        case _: Reset[?]                => "reset"
                      }
                      .take(1L + events1.length)
                      .timeout(2.seconds)
                      .runCollect
                      .fork
                  )
                  _ <- subscription.restartFromFirstEvent(LastEvent)
                  result <- resultFiber.join
                } yield assert(result.toList)(hasSameElements(Seq("reset") ++ events1.asStrings))
              )
              .provideSome[R](repository)
          }
        },
        test(
          "listen should publish switchedToLive event once past events are published and before live events"
        ) {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen, size1Gen = atLeastOne)) { case (firstStreamId, events1, events2) =>
            ZIO
              .scoped {
                for {
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]

                  _ <- repository.saveEvents(firstStreamId, events1)

                  subscription <- repository.listen[Event1]

                  _ <- subscription.restartFromFirstEvent()

                  result <- subscription.stream
                    .tap {
                      case _: Reset[?] => repository.saveEvents(firstStreamId, events2)
                      case _           => ZIO.unit
                    }
                    .collect {
                      case e: RepositoryEvent[Event1] => e.asString
                      case _: SwitchedToLive[?]       => "switch"
                    }
                    .sliding(3)
                    .collect { case c @ Chunk(_, "switch", _) => c }
                    .timeout(1.seconds)
                    .runHead
                } yield assert(result)(
                  isSome(
                    equalTo(
                      Chunk(events1.last.asString, "switch", events2.head.asString)
                    )
                  )
                )
              }
              .provideSome[R](repository)
          }
        },
        test(
          "listen should publish switchedToLive event right after reset and before live events"
        ) {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen, size1Gen = atLeastOne)) { case (firstStreamId, events1, _) =>
            ZIO
              .scoped {
                for {
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]

                  subscription <- repository.listen[Event1]

                  _ <- subscription.restartFromFirstEvent()

                  result <- subscription.stream
                    .tap {
                      case _: Reset[?] => repository.saveEvents(firstStreamId, events1)
                      case _           => ZIO.unit
                    }
                    .collect {
                      case e: RepositoryEvent[Event1] => e.asString
                      case _: SwitchedToLive[?]       => "switch"
                      case _: Reset[?]                => "reset"
                    }
                    .sliding(3)
                    .collect { case c @ Chunk("reset", "switch", _) => c }
                    .timeout(1.seconds)
                    .runHead
                } yield assert(result)(
                  isSome(
                    equalTo(
                      Chunk("reset", "switch", events1.head.asString)
                    )
                  )
                )
              }
              .provideSome[R](repository)
          }
        },
        test(
          "listen should publish new events after a last-event-reset when no events"
        ) {
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          ZIO
            .scoped(
              for {
                repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                subscription <- repository.listen[Event1]
                resultFiber <- Live.live(
                  subscription.stream
                    .collect {
                      case e: RepositoryEvent[Event1] => e.asString
                      case _: Reset[?]                => "reset"
                    }
                    .take(1)
                    .timeout(1.seconds)
                    .runCollect
                    .fork
                )
                _ <- subscription.restartFromFirstEvent(LastEvent)
                result <- resultFiber.join
              } yield assert(result.toList)(hasSameElements(Seq("reset")))
            )
            .provideSome[R](repository)
        },
        test("listen should stream appended events") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen)) { case (firstStreamId, events, _) =>
            ZIO
              .scoped(
                for {
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  subscription <- repository.listen[Event1]
                  _ <- repository.saveEvents(firstStreamId, events)
                  result <- subscription.stream
                    .take(events.length.toLong)
                    .collect { case e: RepositoryEvent[Event1] =>
                      e
                    }
                    .timeout(1.seconds)
                    .runCollect
                } yield assert(result.toList.asRepositoryWriteEvents)(
                  equalTo(events)
                )
              )
              .provideSome[R](repository)
          }
        },
        test("listen should switch to first events") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen, size2Gen = atLeastOne)) { case (firstStreamId, events1, events2) =>
            val nbEvents1 = events1.length.toLong
            val nbEvents2 = events2.length.toLong
            ZIO
              .scoped(
                for {
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  _ <- repository.saveEvents(firstStreamId, events1)
                  subscription <- repository.listen[Event1]
                  _ <- repository.saveEvents(firstStreamId, events2)
                  lastKnownVersionForEvents2 <- repository
                    .getAllEvents[Event1]
                    .flatMap(_.runLast)
                    .map(
                      _.map(_.eventStoreVersion)
                        .getOrElse(EventStoreVersion.initial)
                    )
                  result <- subscription.stream
                    .collect { case e: RepositoryEvent[Event1] =>
                      e
                    }
                    .take(nbEvents1 + nbEvents2 * 2)
                    .tap(event =>
                      ZIO.when(
                        event.eventStoreVersion == lastKnownVersionForEvents2
                      )(
                        subscription.restartFromFirstEvent(
                          Version(lastKnownVersionForEvents2)
                        )
                      )
                    )
                    .timeout(1.seconds)
                    .runCollect
                } yield assert(result.toList.asRepositoryWriteEvents)(
                  equalTo(events2 ++ events1 ++ events2)
                )
              )
              .provideSome[R](repository)
          }
        },
        test(
          "listen should allow several restartFromFirstEvent with same version"
        ) {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen)) { case (firstStreamId, events1, events2) =>
            val nbEvents1 = events1.length.toLong
            val nbEvents2 = events2.length.toLong
            ZIO
              .scoped(
                for {
                  restartedCounter <- Ref.make[Int](0)
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  subscription <- repository.listen[Event1]
                  _ <- repository.saveEvents(firstStreamId, events1)
                  lastKnownVersionForEvents <- repository
                    .getAllEvents[Event1]
                    .flatMap(_.runLast)
                    .map(
                      _.map(_.eventStoreVersion)
                        .getOrElse(EventStoreVersion.initial)
                    )
                  fiber <- subscription.stream
                    .collect { case e: RepositoryEvent[Event1] =>
                      e
                    }
                    .take(nbEvents1 * 3 + nbEvents2)
                    .tap(event =>
                      ZIO.when(
                        event.eventStoreVersion == lastKnownVersionForEvents
                      )(
                        subscription
                          .restartFromFirstEvent(Version(lastKnownVersionForEvents))
                          .executeTwice(restartedCounter)
                      )
                    )
                    .timeout(1.seconds)
                    .runCollect
                    .fork
                  _ <- repository.saveEvents(firstStreamId, events2)
                  result <- fiber.join
                } yield assert(result.toList.asRepositoryWriteEvents)(
                  equalTo(events1 ++ events1 ++ events1 ++ events2)
                )
              )
              .provideSome[R](repository)
          }
        },
        test("listen should switch to first events then next live events") {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen)) { case (firstStreamId, events1, events2) =>
            val nbEvents1 = events1.length.toLong
            val nbEvents2 = events2.length.toLong
            ZIO
              .scoped(
                for {
                  restartedCounter <- Ref.make[Int](0)
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  subscription <- repository.listen[Event1]
                  _ <- repository.saveEvents(firstStreamId, events1)
                  lastKnownVersionForEvents <- repository
                    .getAllEvents[Event1]
                    .flatMap(_.runLast)
                    .map(
                      _.map(_.eventStoreVersion)
                        .getOrElse(EventStoreVersion.initial)
                    )
                  fiber <- subscription.stream
                    .collect { case e: RepositoryEvent[Event1] =>
                      e
                    }
                    .take(nbEvents1 * 2 + nbEvents2)
                    .tap(event =>
                      ZIO.when(
                        event.eventStoreVersion == lastKnownVersionForEvents
                      )(
                        subscription
                          .restartFromFirstEvent(Version(lastKnownVersionForEvents))
                          .executeOnce(restartedCounter)
                      )
                    )
                    .timeout(1.seconds)
                    .runCollect
                    .fork
                  _ <- repository.saveEvents(firstStreamId, events2)
                  result <- fiber.join
                } yield assert(result.toList.asRepositoryWriteEvents)(
                  equalTo(events1 ++ events1 ++ events2)
                )
              )
              .provideSome[R](repository)
          }
        },
        test(
          "listen should read events prior to the listening start then next live events"
        ) {
          implicit val implicitEvent1Encoder: Encoder[Event1] = codecs.event1Encoder
          implicit val implicitEvent1Decoder: Decoder[Event1] = codecs.event1Decoder

          check(eventsGen(event1Gen, size2Gen = atLeastOne)) { case (firstStreamId, events1, events2) =>
            val nbEvents1 = events1.length.toLong
            val nbEvents2 = events2.length.toLong
            ZIO
              .scoped(
                for {
                  restartedCounter <- Ref.make[Int](0)
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  _ <- repository.saveEvents(firstStreamId, events1)
                  lastKnownVersionForEvents <- repository
                    .getAllEvents[Event1]
                    .flatMap(_.runLast)
                    .map(
                      _.map(_.eventStoreVersion)
                        .getOrElse(EventStoreVersion.initial)
                    )
                  subscription <- repository.listen[Event1]
                  fiber <- subscription.stream
                    .collect { case e: RepositoryEvent[Event1] =>
                      e
                    }
                    .take(1 + nbEvents1 + nbEvents2)
                    .tap(event =>
                      ZIO.when(
                        event.eventStoreVersion == lastKnownVersionForEvents.next
                      )(
                        subscription
                          .restartFromFirstEvent(Version(lastKnownVersionForEvents.next))
                          .executeOnce(restartedCounter)
                      )
                    )
                    .timeout(1.seconds)
                    .runCollect
                    .fork
                  _ <- repository.saveEvents(firstStreamId, events2)
                  result <- fiber.join
                } yield assert(result.toList.asRepositoryWriteEvents)(
                  equalTo(events2.take(1) ++ events1 ++ events2)
                )
              )
              .provideSome[R](repository)
          }
        }
      )
    ) @@ TestAspect.shrinks(0)
  } @@ TestAspect.timeout(30.seconds) @@ TestAspect.timed @@ TestAspect.samples(10)

  implicit class Once[R, E, A](self: ZIO[R, E, A]) {
    def executeOnce(store: Ref[Int]) =
      ZIO.whenZIO(store.getAndUpdate(_ + 1).map(_ < 1))(self)

    def executeTwice(store: Ref[Int]) =
      ZIO.whenZIO(store.getAndUpdate(_ + 1).map(_ < 2))(self)
  }

  implicit class RepositoryEventOps[E](
      self: RepositoryEvent[E]
  ) {
    def asString = s"event ${self.aggregateVersion}"
  }

  implicit class RepositoryEventsOps[E](
      self: Seq[RepositoryEvent[E]]
  ) {
    def asStrings = self.map(_.asString)
  }

  implicit class RepositoryWriteEventOps[E](
      self: RepositoryWriteEvent[E]
  ) {
    def asString = s"event ${self.aggregateVersion}"
  }

  implicit class RepositoryWriteEventsOps[E](
      self: Seq[RepositoryWriteEvent[E]]
  ) {
    def asStrings = self.map(_.asString)
  }

  implicit class SubscriptionOps[EventType](self: Subscription[EventType]) {
    def events =
      self.stream
        .collect { case e: RepositoryEvent[EventType] => e }
  }
}
