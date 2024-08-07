package eventstore

import eventstore.types._
import izumi.reflect.Tag
import izumi.reflect.macrortti.LightTypeTag

import java.time.OffsetDateTime

case class RepositoryWriteEvent[+EventType, +DoneBy](
    processId: ProcessId,
    aggregateId: AggregateId,
    aggregateName: AggregateName,
    aggregateVersion: AggregateVersion,
    sentDate: OffsetDateTime,
    doneBy: DoneBy,
    event: EventType
)

sealed trait EventStoreEvent[+EventType, +DoneBy]
case class RepositoryEvent[+EventType: Tag, +DoneBy: Tag](
    processId: ProcessId,
    aggregateId: AggregateId,
    aggregateName: AggregateName,
    aggregateVersion: AggregateVersion,
    sentDate: OffsetDateTime,
    eventStoreVersion: EventStoreVersion,
    doneBy: DoneBy,
    event: EventType
) extends EventStoreEvent[EventType, DoneBy] {
  private[eventstore] def eventTag: LightTypeTag = implicitly[Tag[EventType]].tag

  private[eventstore] def doneByTag: LightTypeTag = implicitly[Tag[DoneBy]].tag
}

case class Reset[+EventType, +DoneBy]() extends EventStoreEvent[EventType, DoneBy]
