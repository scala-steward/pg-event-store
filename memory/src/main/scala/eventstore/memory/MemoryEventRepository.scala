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
import zio.Ref
import zio._
import zio.stream.Stream
import zio.stream.ZStream

import EventRepository.Error.Unexpected
import EventRepository.Error.VersionConflict
import EventRepository.{EventsOps, SaveEventError, Subscription}

class MemoryEventRepository[UnusedDecoder[_], UnusedEncoder[_]](
    storageRef: Ref[Storage],
    hub: Hub[RepositoryEvent[Any, Any]]
) extends EventRepository[UnusedDecoder, UnusedEncoder] {

  override def getEventStream[A: UnusedDecoder: Tag, DoneBy: UnusedDecoder: Tag](
      eventStreamId: EventStreamId
  ): IO[Unexpected, Seq[RepositoryEvent[A, DoneBy]]] = storageRef.get.map(_.getEvents(eventStreamId))

  override def saveEvents[A: UnusedDecoder: UnusedEncoder: Tag, DoneBy: UnusedDecoder: UnusedEncoder: Tag](
      eventStreamId: EventStreamId,
      newEvents: Seq[RepositoryWriteEvent[A, DoneBy]]
  ): IO[SaveEventError, Seq[RepositoryEvent[A, DoneBy]]] = for {

    _ <- newEvents.checkVersionsAreContiguousIncrements

    storage <- storageRef.get

    result <- storage.appendEvents(eventStreamId, newEvents)
    (updatedStorage, newRepositoryEvents) = result

    _ <- storageRef.set(updatedStorage)
    _ <- hub.publishAll(newRepositoryEvents)
  } yield newRepositoryEvents

  override def getAllEvents[EventType: UnusedDecoder: Tag, DoneBy: UnusedDecoder: Tag]
      : ZStream[Any, Unexpected, RepositoryEvent[EventType, DoneBy]] =
    ZStream.unwrap {
      for { events <- storageRef.get.map(_.events) } yield ZStream
        .fromIterable(events)
        .map(_.asInstanceOf[RepositoryEvent[EventType, DoneBy]])
    }

  override def listEventStreamWithName(aggregateName: AggregateName): Stream[Unexpected, EventStreamId] =
    ZStream.fromIterableZIO(
      storageRef.get.map(
        _.byAggregate.keys.filter(_.aggregateName == aggregateName)
      )
    )

  override def listen[EventType: UnusedDecoder: Tag, DoneBy: UnusedDecoder: Tag]
      : ZIO[Scope, Unexpected, Subscription[EventType, DoneBy]] = {
    val fromDb = ZIO.succeed(getAllEvents[EventType, DoneBy])

    val typeTag = implicitly[Tag[EventType]]
    val doneTag = implicitly[Tag[DoneBy]]

    for {
      live <- hub.subscribe.map { subscription =>
        ZStream
          .fromQueue(subscription)
          .collect {
            case event: RepositoryEvent[Any, Any]
                if event.eventTag <:< typeTag.tag && event.doneByTag <:< doneTag.tag =>
              event.asInstanceOf[RepositoryEvent[EventType, DoneBy]]
          }
      }

      switchableStream <- SwitchableZStream.from(live, fromDb)

    } yield Subscription.fromSwitchableStream(switchableStream)
  }

}

object MemoryEventRepository {

  type Id[A] = Unit

  case class Storage(
      events: List[RepositoryEvent[?, ?]],
      byAggregate: Map[EventStreamId, List[RepositoryEvent[?, ?]]]
  ) {

    def appendEvents[A: Tag, DoneBy: Tag](
        eventStreamId: EventStreamId,
        newEvents: Seq[RepositoryWriteEvent[A, DoneBy]]
    ): IO[SaveEventError, (Storage, Seq[RepositoryEvent[A, DoneBy]])] = {
      val currentEvents = getEvents(eventStreamId)
      for {
        _ <- checkExpectedVersion(currentEvents, newEvents)
        eventStoreVersion = currentEvents.lastOption.map(_.eventStoreVersion).getOrElse(EventStoreVersion.initial)
        newRepositoryEvents = newEvents.toRepositoryEvents(eventStoreVersion)
      } yield copy(
        byAggregate = byAggregate.updated(eventStreamId, currentEvents ++ newRepositoryEvents),
        events = events ++ newRepositoryEvents
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
          ZIO
            .fail[SaveEventError](VersionConflict(headVersion, expectedVersion))
            .unless(headVersion == expectedVersion)
        }
        .getOrElse(ZIO.unit)
    }

    def getEvents[A, DoneBy](eventStreamId: EventStreamId): List[RepositoryEvent[A, DoneBy]] =
      byAggregate
        .getOrElse(key = eventStreamId, default = List.empty)
        .asInstanceOf[List[RepositoryEvent[A, DoneBy]]]

  }

  def layer[UnusedDecoder[_]: TagK, UnusedEncoder[_]: TagK]: ULayer[EventRepository[UnusedDecoder, UnusedEncoder]] = {
    ZLayer {
      for {
        map <- Ref.make(Storage(List.empty, Map.empty))
        hub <- Hub.unbounded[RepositoryEvent[Any, Any]]
      } yield new MemoryEventRepository(map, hub)
    }
  }
}
