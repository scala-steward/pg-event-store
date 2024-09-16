package eventstore.playjson.pg
import doobie._
import eventstore.EventRepository
import eventstore.RepositoryEvent
import eventstore.RepositoryWriteEvent
import eventstore.types.AggregateName
import eventstore.types.EventStreamId
import play.api.libs.json.Json
import play.api.libs.json.Reads
import play.api.libs.json.Writes
import zio.ZIO
import zio._
import zio.stream.ZStream

import EventRepository.Error

private class PostgresPlayJsonEventRepository(
    postgresEventRepositoryLive: EventRepository[Get, Put]
) extends EventRepository[Reads, Writes] {

  private def getJson[A: Reads: Tag]: Get[A] =
    Get[String].temap(input =>
      Json
        .parse(input)
        .validate[A]
        .asEither
        .left
        .map(msg => s"trying to decode $input as ${implicitly[Tag[A]].tag}, error $msg")
    )

  private def putJson[A](implicit writes: Writes[A]): Put[A] =
    Put[String].contramap(obj => Json.stringify(writes.writes(obj)))

  override def getEventStream[EventType: Reads: Tag, DoneBy: Reads: Tag](
      eventStreamId: EventStreamId
  ): IO[Error.Unexpected, Seq[RepositoryEvent[EventType, DoneBy]]] = {
    implicit val getEventType: Get[EventType] = getJson[EventType]
    implicit val getDoneBy: Get[DoneBy] = getJson[DoneBy]
    postgresEventRepositoryLive.getEventStream[EventType, DoneBy](eventStreamId)
  }

  override def saveEvents[EventType: Reads: Writes: Tag, DoneBy: Reads: Writes: Tag](
      eventStreamId: EventStreamId,
      events: Seq[RepositoryWriteEvent[EventType, DoneBy]]
  ): IO[EventRepository.SaveEventError, Seq[RepositoryEvent[EventType, DoneBy]]] = {
    implicit val getEventType: Get[EventType] = getJson[EventType]
    implicit val putEventType: Put[EventType] = putJson[EventType]
    implicit val getDoneBy: Get[DoneBy] = getJson[DoneBy]
    implicit val putDoneBy: Put[DoneBy] = putJson[DoneBy]
    postgresEventRepositoryLive.saveEvents(eventStreamId, events)
  }

  override def listen[EventType: Reads: Tag, DoneBy: Reads: Tag]: ZIO[
    Scope,
    Error.Unexpected,
    EventRepository.Subscription[EventType, DoneBy]
  ] = {
    implicit val getEventType: Get[EventType] = getJson[EventType]
    implicit val getDoneBy: Get[DoneBy] = getJson[DoneBy]
    postgresEventRepositoryLive.listen[EventType, DoneBy]

  }

  override def getAllEvents[A: Reads: Tag, DoneBy: Reads: Tag]
      : ZStream[Any, Error.Unexpected, RepositoryEvent[A, DoneBy]] = {
    implicit val getEventType: Get[A] = getJson[A]
    implicit val getDoneBy: Get[DoneBy] = getJson[DoneBy]
    postgresEventRepositoryLive.getAllEvents[A, DoneBy]
  }

  override def listEventStreamWithName(
      aggregateName: AggregateName
  ): ZStream[Any, Error.Unexpected, EventStreamId] =
    postgresEventRepositoryLive.listEventStreamWithName(aggregateName)
}

object PostgresPlayJsonEventRepository {
  def layer: URLayer[EventRepository[Get, Put], EventRepository[Reads, Writes]] =
    ZLayer.derive[PostgresPlayJsonEventRepository]
}