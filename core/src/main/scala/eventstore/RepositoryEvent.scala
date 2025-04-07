package eventstore

import eventstore.types._
import izumi.reflect.Tag
import izumi.reflect.macrortti.LightTypeTag

import java.time.OffsetDateTime

case class RepositoryWriteEvent[+EventType](
    processId: ProcessId,
    aggregateId: AggregateId,
    aggregateName: AggregateName,
    aggregateVersion: AggregateVersion,
    sentDate: OffsetDateTime,
    event: EventType
)

sealed trait EventStoreEvent[+EventType]
case class RepositoryEvent[+EventType: Tag](
    processId: ProcessId,
    aggregateId: AggregateId,
    aggregateName: AggregateName,
    aggregateVersion: AggregateVersion,
    sentDate: OffsetDateTime,
    eventStoreVersion: EventStoreVersion,
    event: EventType
) extends EventStoreEvent[EventType] {
  private[eventstore] def eventTag: LightTypeTag = implicitly[Tag[EventType]].tag
}

case class Reset[+EventType]() extends EventStoreEvent[EventType]

case class SwitchedToLive[+EventType]() extends EventStoreEvent[EventType]
