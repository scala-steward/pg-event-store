package eventstore.memory

import eventstore.EventRepository
import eventstore.RepositoryEvent
import eventstore.RepositoryWriteEvent
import eventstore.SwitchableZStream
import eventstore.memory.MemoryEventRepository.Storage
import eventstore.types.AggregateName
import eventstore.types.AggregateVersion
import eventstore.types.EventStoreVersion
import eventstore.types.EventStreamId
import zio._
import zio.stm.TRef
import zio.stm.ZSTM
import zio.stream.Stream
import zio.stream.ZStream

import scala.collection.immutable.ListSet
import scala.math.Ordered.orderingToOrdered

import EventRepository.Error.Unexpected
import EventRepository.Error.VersionConflict
import EventRepository.{Direction, EventsOps, SaveEventError, Subscription}

class MemoryEventRepository[UnusedDecoder[_], UnusedEncoder[_]](
    storageRef: TRef[Storage],
    hub: Hub[RepositoryEvent[Any, Any]]
) extends EventRepository[UnusedDecoder, UnusedEncoder] {

  override def getEventStream[A: UnusedDecoder: Tag, DoneBy: UnusedDecoder: Tag](
      eventStreamId: EventStreamId,
      direction: Direction = Direction.Forward
  ): ZIO[Scope, Unexpected, Stream[Unexpected, RepositoryEvent[A, DoneBy]]] =
    storageRef.get
      .map(_.getEvents(eventStreamId))
      .commit
      .map(events =>
        direction match {
          case Direction.Forward  => events
          case Direction.Backward => events.reverse
        }
      )
      .map(x => ZStream.fromIterable(x))

  override def saveEvents[A: UnusedDecoder: UnusedEncoder: Tag, DoneBy: UnusedDecoder: UnusedEncoder: Tag](
      eventStreamId: EventStreamId,
      newEvents: Seq[RepositoryWriteEvent[A, DoneBy]]
  ): IO[SaveEventError, Seq[RepositoryEvent[A, DoneBy]]] = for {

    events <- (for {
      _ <- ZSTM.fromEither(newEvents.checkVersionsAreContiguousIncrements)

      storage <- storageRef.get

      result <- storage.appendEvents(eventStreamId, newEvents)
      (updatedStorage, newRepositoryEvents) = result

      _ <- storageRef.set(updatedStorage)
    } yield newRepositoryEvents).commit
    _ <- hub.publishAll(events)
  } yield events

  override def getAllEvents[EventType: UnusedDecoder: Tag, DoneBy: UnusedDecoder: Tag]
      : ZIO[Scope, Nothing, Stream[Unexpected, RepositoryEvent[EventType, DoneBy]]] =
    for { events <- storageRef.get.map(_.events).commit } yield {
      ZStream
        .fromIterable(events)
        .map(_.asInstanceOf[RepositoryEvent[EventType, DoneBy]])
    }

  override def listEventStreamWithName(
      aggregateName: AggregateName,
      direction: Direction = Direction.Forward
  ): Stream[Unexpected, EventStreamId] =
    ZStream.fromIterableZIO(
      for { aggregates <- storageRef.get.map(_.aggregates).commit } yield {
        val naturalyOrdered = aggregates.filter(_.aggregateName == aggregateName).toList
        direction match {
          case Direction.Forward  => naturalyOrdered
          case Direction.Backward => naturalyOrdered.reverse
        }
      }
    )

  override def listen[EventType: UnusedDecoder: Tag, DoneBy: UnusedDecoder: Tag]
      : ZIO[Scope, Unexpected, Subscription[EventType, DoneBy]] = listenImpl(live = fromHub[EventType, DoneBy])

  override def listenFromVersion[EventType: UnusedDecoder: Tag, DoneBy: UnusedDecoder: Tag](
      fromExclusive: EventStoreVersion
  ): ZIO[Scope, Unexpected, Subscription[EventType, DoneBy]] = {
    val live = for {
      fromDb <- getAllEvents[EventType, DoneBy]
      fromHub <- fromHub[EventType, DoneBy]
    } yield fromDb.concat(fromHub).dropWhile(_.eventStoreVersion <= fromExclusive)
    listenImpl(live = live)
  }

  private def listenImpl[EventType: UnusedDecoder: Tag, DoneBy: UnusedDecoder: Tag](
      live: ZIO[Scope, Nothing, ZStream[Any, Unexpected, RepositoryEvent[EventType, DoneBy]]]
  ): ZIO[Scope, Unexpected, Subscription[EventType, DoneBy]] = {
    val fromDb = getAllEvents[EventType, DoneBy]

    for {
      live <- live
      switchableStream <- SwitchableZStream.from(live, fromDb)
    } yield Subscription.fromSwitchableStream(switchableStream, getLastEventVersion)
  }

  private def fromHub[EventType: UnusedDecoder: Tag, DoneBy: UnusedDecoder: Tag]
      : ZIO[Scope, Nothing, ZStream[Any, Nothing, RepositoryEvent[EventType, DoneBy]]] = {
    val typeTag = implicitly[Tag[EventType]]
    val doneTag = implicitly[Tag[DoneBy]]
    hub.subscribe.map { subscription =>
      ZStream
        .fromQueue(subscription)
        .collect {
          case event: RepositoryEvent[Any, Any] if event.eventTag <:< typeTag.tag && event.doneByTag <:< doneTag.tag =>
            event.asInstanceOf[RepositoryEvent[EventType, DoneBy]]
        }
    }

  }

  private def getLastEventVersion: IO[Unexpected, Option[EventStoreVersion]] =
    storageRef.get.map(_.events.lastOption.map(_.eventStoreVersion)).commit

}

object MemoryEventRepository {

  type Id[A] = Unit

  case class Storage(
      events: List[RepositoryEvent[?, ?]],
      byAggregate: Map[EventStreamId, List[RepositoryEvent[?, ?]]],
      aggregates: ListSet[EventStreamId]
  ) {

    def appendEvents[A: Tag, DoneBy: Tag](
        eventStreamId: EventStreamId,
        newEvents: Seq[RepositoryWriteEvent[A, DoneBy]]
    ): ZSTM[Any, SaveEventError, (Storage, Seq[RepositoryEvent[A, DoneBy]])] = {
      val currentEvents = getEvents(eventStreamId)
      for {
        _ <- checkExpectedVersion(currentEvents, newEvents)
        eventStoreVersion = currentEvents.lastOption.map(_.eventStoreVersion).getOrElse(EventStoreVersion.initial)
        newRepositoryEvents = newEvents.toRepositoryEvents(eventStoreVersion)
      } yield copy(
        byAggregate = byAggregate.updated(eventStreamId, currentEvents ++ newRepositoryEvents),
        events = events ++ newRepositoryEvents,
        aggregates = aggregates + eventStreamId
      ) -> newRepositoryEvents
    }

    implicit class EventsOps[A: Tag, DoneBy: Tag](self: Seq[RepositoryWriteEvent[A, DoneBy]]) {
      def toRepositoryEvents(eventStoreVersion: EventStoreVersion): Seq[RepositoryEvent[A, DoneBy]] =
        self
          .zip(LazyList.iterate(eventStoreVersion.next)(v => v.next))
          .map { case (evt, version) =>
            RepositoryEvent(
              evt.processId,
              evt.aggregateId,
              evt.aggregateName,
              evt.aggregateVersion,
              evt.sentDate,
              version,
              evt.doneBy,
              evt.event
            )
          }
    }

    private def checkExpectedVersion(
        currentEvents: Seq[RepositoryEvent[?, ?]],
        newEvents: Seq[RepositoryWriteEvent[?, ?]]
    ) = {
      newEvents.headOption
        .map { headEvent =>
          val headVersion = headEvent.aggregateVersion
          val expectedVersion = {
            currentEvents.lastOption
              .map(_.aggregateVersion.next)
              .getOrElse(AggregateVersion.initial)
          }
          ZSTM
            .fail[SaveEventError](VersionConflict(headVersion, expectedVersion))
            .unless(headVersion == expectedVersion)
        }
        .getOrElse(ZSTM.unit)
    }

    def getEvents[A, DoneBy](eventStreamId: EventStreamId): List[RepositoryEvent[A, DoneBy]] =
      byAggregate
        .getOrElse(key = eventStreamId, default = List.empty)
        .asInstanceOf[List[RepositoryEvent[A, DoneBy]]]

  }

  def layer[UnusedDecoder[_]: TagK, UnusedEncoder[_]: TagK]: ULayer[EventRepository[UnusedDecoder, UnusedEncoder]] = {
    ZLayer {
      for {
        map <- TRef.makeCommit(Storage(List.empty, Map.empty, ListSet.empty))
        hub <- Hub.unbounded[RepositoryEvent[Any, Any]]
      } yield new MemoryEventRepository(map, hub)
    }
  }
}
