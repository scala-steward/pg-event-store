package eventstore

import types.{AggregateId, AggregateName, AggregateVersion, EventStoreVersion, EventStreamId, ProcessId}
import EventRepository.Error.{Unexpected, VersionConflict}
import EventRepository.Subscription
import zio.stream.{UStream, ZStream}
import zio.{Chunk, Random, Ref, Tag, TagK, URLayer, ZIO, durationInt}
import zio.test.*
import zio.test.Assertion.*
import zio.test.magnolia.DeriveGen

import java.time.OffsetDateTime

object EventRepositorySpec {
  case class User(id: String)

  sealed trait Event

  sealed trait Event1 extends Event
  case object A extends Event1
  case class B(foo: Boolean, bar: Int) extends Event1

  sealed trait Event2 extends Event
  case object C extends Event2
  case class D(foo: Boolean, bar: Int) extends Event2

  implicit class WriteEventOps[+E: Tag, +DoneBy: Tag](
      events: Seq[RepositoryEvent[E, DoneBy]]
  ) {
    def asRepositoryWriteEvents =
      events.map(event =>
        RepositoryWriteEvent(
          processId = event.processId,
          aggregateId = event.aggregateId,
          aggregateName = event.aggregateName,
          aggregateVersion = event.aggregateVersion,
          sentDate = event.sentDate,
          doneBy = event.doneBy,
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
        userId <- Random.nextUUID.map(x => User(x.toString))
      } yield RepositoryEvent[A, User](
        processId = processId,
        aggregateId = streamId.aggregateId,
        aggregateName = streamId.aggregateName,
        sentDate = OffsetDateTime.parse("2027-12-03T10:15:30+01:00"),
        doneBy = userId,
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
        userId <- Random.nextUUID.map(x => User(x.toString))
      } yield RepositoryWriteEvent[A, User](
        processId = processId,
        aggregateId = streamId.aggregateId,
        aggregateName = streamId.aggregateName,
        sentDate = OffsetDateTime.parse("2027-12-03T10:15:30+01:00"),
        doneBy = userId,
        aggregateVersion = version,
        event = a
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
  val eventGen: Gen[Any, Event1] = DeriveGen.gen[Event1].derive
  val event2Gen: Gen[Any, Event2] = DeriveGen.gen[Event2].derive

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
      size1Gen: Gen[Any, Int] = Gen.int(1, 20),
      size2Gen: Gen[Any, Int] = Gen.int(1, 20)
  ): Gen[
    Any,
    (
        EventStreamId,
        List[RepositoryWriteEvent[EventType, User]],
        List[RepositoryWriteEvent[EventType, User]]
    )
  ] = {
    for {
      streamId <- streamIdGen(nameGen)
      size1 <- size1Gen
      size2 <- size2Gen
      events <- Gen.unfoldGenN(size1 + size2)(AggregateVersion.initial)(version => {
        eventGen.mapZIO(event => {
          event
            .asRepositoryWriteEvent(version = version, streamId = streamId)
            .map(event => version.next -> event)
        })
      })
      (events1, events2) = events.splitAt(size1)
    } yield (streamId, events1, events2)
  }

  case class Codecs[Decoder[_], Encoder[_]](
      eventDecoder: Decoder[Event],
      event1Decoder: Decoder[Event1],
      event1Encoder: Encoder[Event1],
      event2Decoder: Decoder[Event2],
      event2Encoder: Encoder[Event2],
      userDecoder: Decoder[User],
      userEncoder: Encoder[User]
  ) {
    implicit val implicitEventDecoder: Decoder[Event] = eventDecoder
    implicit val implicitEvent1Decoder: Decoder[Event1] = event1Decoder
    implicit val implicitEvent1Encoder: Encoder[Event1] = event1Encoder
    implicit val implicitEvent2Decoder: Decoder[Event2] = event2Decoder
    implicit val implicitEvent2Encoder: Encoder[Event2] = event2Encoder
    implicit val implicitUserDecoder: Decoder[User] = userDecoder
    implicit val implicitUserEncoder: Encoder[User] = userEncoder
  }

  def spec[R, Decoder[_]: TagK, Encoder[_]: TagK](
      repository: URLayer[R, EventRepository[Decoder, Encoder]]
  )(implicit codecs: Codecs[Decoder, Encoder]) = {
    import codecs._
    suite("EventRepository spec")(
      suite("examples")(
        test("should not fail when saving a single event") {
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
          for {
            firstStreamId <- AggregateId.generate.map(aggregateId => {
              EventStreamId(
                aggregateId = aggregateId,
                aggregateName = AggregateName("Foo")
              )
            })
            _ <- ZIO.serviceWithZIO[EventRepository[Decoder, Encoder]](
              _.saveEvents[Event1, User](firstStreamId, Seq.empty)
            )
          } yield assertCompletes
        },
        test(
          "getEventStream should return empty seq when eventStreamId doesn't exist"
        ) {
          for {
            firstStreamId <- AggregateId.generate.map(aggregateId => {
              EventStreamId(
                aggregateId = aggregateId,
                aggregateName = AggregateName("Foo")
              )
            })
            result <- ZIO.serviceWithZIO[EventRepository[Decoder, Encoder]](
              _.getEventStream[Event1, User](firstStreamId)
            )
          } yield assert(result)(isEmpty)
        },
        test("should dispatch events to all listeners") {
          ZIO.scoped {
            for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              listener1 <- repository.listen[Event1, User].map(_.events)
              listener2 <- repository.listen[Event1, User].map(_.events)
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
      ).provideSome[R](repository),
      suite("properties")(
        test("should fail when aggregateVersion is not previous.next") {
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
              savedEvents <- repository
                .saveEvents[Event1, User](streamId, Seq(event))
              secondEvent <- A
                .asRepositoryWriteEvent(version = v, streamId = streamId)
              error <- repository
                .saveEvents[Event1, User](streamId, Seq(secondEvent))
                .either
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
          check(versionGen.zip(versionGen).filterNot { case (v1, v2) =>
            v1.next == v2
          }) { case (v1, v2) =>
            (for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              streamId <- AggregateId.generate.map(aggregateId => {
                EventStreamId(
                  aggregateId = aggregateId,
                  aggregateName = AggregateName("Foo")
                )
              })
              events <- ZIO.collectAll(
                Seq(
                  A.asRepositoryWriteEvent(
                    streamId = streamId,
                    version = AggregateVersion.initial
                  ),
                  A
                    .asRepositoryWriteEvent(streamId = streamId, version = v1),
                  A
                    .asRepositoryWriteEvent(streamId = streamId, version = v2)
                )
              )
              error <- repository
                .saveEvents[Event1, User](streamId, events)
                .either
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
          check(eventsGen(eventGen)) { case (firstStreamId, events, _) =>
            (for {
              repository <- ZIO.service[EventRepository[Decoder, Encoder]]
              _ <- repository.saveEvents(firstStreamId, events)
              result <- repository.getEventStream[Event1, User](firstStreamId)
            } yield assert(result.asRepositoryWriteEvents)(equalTo(events)))
              .provideSome[R](repository)
          }
        },
        test("listen should stream appended events") {
          check(eventsGen(eventGen)) { case (firstStreamId, events, _) =>
            ZIO
              .scoped(for {
                repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                stream <- repository.listen[Event1, User].map(_.events)
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
          check(eventsGen(eventGen), eventsGen(event2Gen)) {
            case ((secondStreamId, events, _), (firstStreamId, events2, _)) =>
              ZIO
                .scoped(
                  for {
                    repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                    stream <- repository.listen[Event1, User].map(_.events)
                    _ <- repository.saveEvents(firstStreamId, events2)
                    _ <- repository.saveEvents(secondStreamId, events)
                    result <- stream
                      .take(events.length.toLong)
                      .timeout(1.seconds)
                      .runCollect
                  } yield assert(result.toList.asRepositoryWriteEvents)(
                    equalTo(events)
                  )
                )
                .provideSome[R](repository)
          }
        }, {
          def save(repositoryEvent: RepositoryWriteEvent[Any, User]) = {
            import repositoryEvent._
            def save[EE: Decoder: Encoder: Tag](event: EE) = {
              ZIO.serviceWithZIO[EventRepository[Decoder, Encoder]](
                _.saveEvents[EE, User](
                  EventStreamId(aggregateId, aggregateName),
                  List(
                    RepositoryWriteEvent[EE, User](
                      processId = processId,
                      aggregateId = aggregateId,
                      aggregateName = aggregateName,
                      aggregateVersion = aggregateVersion,
                      sentDate = sentDate,
                      doneBy = doneBy,
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
              check(eventsGen(eventGen), eventsGen(event2Gen)) { case ((_, events1, _), (_, events2, _)) =>
                ZIO
                  .scoped {
                    for {
                      repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                      events <- pickRandomly(
                        List(events1, events2)
                      ).runCollect
                      _ <- ZIO.foreachDiscard(events)(save)
                      result <- repository
                        .getAllEvents[Event, User]
                        .runCollect
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
              check(eventsGen(eventGen), eventsGen(event2Gen)) { case ((_, events1, _), (_, events2, _)) =>
                ZIO
                  .scoped {
                    for {
                      repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                      stream <- repository.listen[Event, User].map(_.events)
                      _ <- pickRandomly(List(events1, events2))
                        .runForeach(save)
                      fromListen <- stream
                        .take(events1.length.toLong + events2.length)
                        .runCollect
                      result <- repository
                        .getAllEvents[Event, User]
                        .runCollect
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
          check(eventsGen(eventGen)) { case (firstStreamId, events, _) =>
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
          check(eventsGen(eventGen).flatMap { case a @ (streamId, _, _) =>
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
          check(
            eventsGen(eventGen, Gen.const(AggregateName("Foo"))),
            eventsGen(eventGen, Gen.const(AggregateName("Foo"))),
            eventsGen(
              eventGen,
              aggregateNameGen.filter(_ != AggregateName("Foo"))
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
        }
      ),
      suite(
        "listen Spec - Pouvoir switcher entre le flux des évènements en cours et un getAll - Pour le rebuild des vues"
      )(
        test(
          "listen should publish reset event before feeding events from the past"
        ) {
          check(eventsGen(eventGen)) { case (firstStreamId, events1, events2) =>
            val nbEvents1 = events1.length.toLong
            val nbEvents2 = events2.length.toLong
            ZIO
              .scoped(
                for {
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  _ <- repository.saveEvents(firstStreamId, events1)
                  subscription <- repository.listen[Event1, User]
                  _ <- repository.saveEvents(firstStreamId, events2)
                  lastKnownVersionForEvents2 <- repository
                    .getAllEvents[Event1, User]
                    .runLast
                    .map(
                      _.map(_.eventStoreVersion)
                        .getOrElse(EventStoreVersion.initial)
                    )
                  result <- subscription.stream
                    .tap {
                      case e: RepositoryEvent[Event1, User] =>
                        ZIO.when(
                          e.eventStoreVersion == lastKnownVersionForEvents2
                        )(
                          subscription.restartFromFirstEvent(
                            lastKnownVersionForEvents2
                          )
                        )
                      case _ => ZIO.unit
                    }
                    .map {
                      case e: RepositoryEvent[Event1, User] => e.asString
                      case _: Reset[?, ?]                   => "reset"
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
        test("listen should stream appended events") {
          check(eventsGen(eventGen)) { case (firstStreamId, events, _) =>
            ZIO
              .scoped(
                for {
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  subscription <- repository.listen[Event1, User]
                  _ <- repository.saveEvents(firstStreamId, events)
                  result <- subscription.stream
                    .take(events.length.toLong)
                    .collect { case e: RepositoryEvent[Event1, User] =>
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
          check(eventsGen(eventGen)) { case (firstStreamId, events1, events2) =>
            val nbEvents1 = events1.length.toLong
            val nbEvents2 = events2.length.toLong
            ZIO
              .scoped(
                for {
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  _ <- repository.saveEvents(firstStreamId, events1)
                  subscription <- repository.listen[Event1, User]
                  _ <- repository.saveEvents(firstStreamId, events2)
                  lastKnownVersionForEvents2 <- repository
                    .getAllEvents[Event1, User]
                    .runLast
                    .map(
                      _.map(_.eventStoreVersion)
                        .getOrElse(EventStoreVersion.initial)
                    )
                  result <- subscription.stream
                    .collect { case e: RepositoryEvent[Event1, User] =>
                      e
                    }
                    .take(nbEvents1 + nbEvents2 * 2)
                    .tap(event =>
                      ZIO.when(
                        event.eventStoreVersion == lastKnownVersionForEvents2
                      )(
                        subscription.restartFromFirstEvent(
                          lastKnownVersionForEvents2
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
          check(eventsGen(eventGen)) { case (firstStreamId, events1, events2) =>
            val nbEvents1 = events1.length.toLong
            val nbEvents2 = events2.length.toLong
            ZIO
              .scoped(
                for {
                  restartedCounter <- Ref.make[Int](0)
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  subscription <- repository.listen[Event1, User]
                  _ <- repository.saveEvents(firstStreamId, events1)
                  lastKnownVersionForEvents <- repository
                    .getAllEvents[Event1, User]
                    .runLast
                    .map(
                      _.map(_.eventStoreVersion)
                        .getOrElse(EventStoreVersion.initial)
                    )
                  fiber <- subscription.stream
                    .collect { case e: RepositoryEvent[Event1, User] =>
                      e
                    }
                    .take(nbEvents1 * 3 + nbEvents2)
                    .tap(event =>
                      ZIO.when(
                        event.eventStoreVersion == lastKnownVersionForEvents
                      )(
                        subscription
                          .restartFromFirstEvent(lastKnownVersionForEvents)
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
          check(eventsGen(eventGen)) { case (firstStreamId, events1, events2) =>
            val nbEvents1 = events1.length.toLong
            val nbEvents2 = events2.length.toLong
            ZIO
              .scoped(
                for {
                  restartedCounter <- Ref.make[Int](0)
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  subscription <- repository.listen[Event1, User]
                  _ <- repository.saveEvents(firstStreamId, events1)
                  lastKnownVersionForEvents <- repository
                    .getAllEvents[Event1, User]
                    .runLast
                    .map(
                      _.map(_.eventStoreVersion)
                        .getOrElse(EventStoreVersion.initial)
                    )
                  fiber <- subscription.stream
                    .collect { case e: RepositoryEvent[Event1, User] =>
                      e
                    }
                    .take(nbEvents1 * 2 + nbEvents2)
                    .tap(event =>
                      ZIO.when(
                        event.eventStoreVersion == lastKnownVersionForEvents
                      )(
                        subscription
                          .restartFromFirstEvent(lastKnownVersionForEvents)
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
          check(eventsGen(eventGen)) { case (firstStreamId, events1, events2) =>
            val nbEvents1 = events1.length.toLong
            val nbEvents2 = events2.length.toLong
            ZIO
              .scoped(
                for {
                  restartedCounter <- Ref.make[Int](0)
                  repository <- ZIO.service[EventRepository[Decoder, Encoder]]
                  _ <- repository.saveEvents(firstStreamId, events1)
                  lastKnownVersionForEvents <- repository
                    .getAllEvents[Event1, User]
                    .runLast
                    .map(
                      _.map(_.eventStoreVersion)
                        .getOrElse(EventStoreVersion.initial)
                    )
                  subscription <- repository.listen[Event1, User]
                  fiber <- subscription.stream
                    .collect { case e: RepositoryEvent[Event1, User] =>
                      e
                    }
                    .take(1 + nbEvents1 + nbEvents2)
                    .tap(event =>
                      ZIO.when(
                        event.eventStoreVersion == lastKnownVersionForEvents.next
                      )(
                        subscription
                          .restartFromFirstEvent(lastKnownVersionForEvents.next)
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
  } @@ TestAspect.timed @@ TestAspect.samples(10)

  implicit class Once[R, E, A](self: ZIO[R, E, A]) {
    def executeOnce(store: Ref[Int]) =
      ZIO.whenZIO(store.getAndUpdate(_ + 1).map(_ < 1))(self)

    def executeTwice(store: Ref[Int]) =
      ZIO.whenZIO(store.getAndUpdate(_ + 1).map(_ < 2))(self)
  }

  implicit class RepositoryEventOps[E, DoneBy](
      self: RepositoryEvent[E, DoneBy]
  ) {
    def asString = s"event ${self.aggregateVersion}"
  }

  implicit class RepositoryEventsOps[E, DoneBy](
      self: Seq[RepositoryEvent[E, DoneBy]]
  ) {
    def asStrings = self.map(_.asString)
  }

  implicit class RepositoryWriteEventOps[E, DoneBy](
      self: RepositoryWriteEvent[E, DoneBy]
  ) {
    def asString = s"event ${self.aggregateVersion}"
  }

  implicit class RepositoryWriteEventsOps[E, DoneBy](
      self: Seq[RepositoryWriteEvent[E, DoneBy]]
  ) {
    def asStrings = self.map(_.asString)
  }

  implicit class SubscriptionOps[EventType, DoneBy](self: Subscription[EventType, DoneBy]) {
    def events =
      self.stream
        .collect { case e: RepositoryEvent[EventType, DoneBy] => e }
  }
}
