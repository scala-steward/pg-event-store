package eventstore

import eventstore.types.AggregateVersion
import eventstore.types.EventStoreVersion
import eventstore.types.EventStreamId
import eventstore.types.ProcessId
import zio.Tag
import zio.ZIO

import java.time.OffsetDateTime

object RepositoryEventOps {

  implicit class Ops[A: Tag](a: A) {
    def asRepositoryEvent(
        version: AggregateVersion = AggregateVersion.initial,
        eventStoreVersion: EventStoreVersion = EventStoreVersion.initial,
        streamId: EventStreamId
    ): ZIO[Any, Nothing, RepositoryEvent[A]] = {
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
    ): ZIO[Any, Nothing, RepositoryWriteEvent[A]] = {
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
    ): ZIO[Any, Nothing, RepositoryWriteEvent[(A, U)]] = {
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

  implicit class WriteEventsOps[+E](
      events: Seq[RepositoryEvent[E]]
  ) {
    def asRepositoryWriteEvents: Seq[RepositoryWriteEvent[E]] =
      events.map(_.asRepositoryWriteEvent)
  }

  implicit class WriteEventOps[+E](
      event: RepositoryEvent[E]
  ) {
    def asRepositoryWriteEvent =
      RepositoryWriteEvent(
        processId = event.processId,
        aggregateId = event.aggregateId,
        aggregateName = event.aggregateName,
        aggregateVersion = event.aggregateVersion,
        sentDate = event.sentDate,
        event = event.event
      )
  }

}
