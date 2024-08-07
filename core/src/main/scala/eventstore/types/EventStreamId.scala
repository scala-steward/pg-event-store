package eventstore.types

case class EventStreamId(aggregateId: AggregateId, aggregateName: AggregateName)
