package eventstore.playjson.pg
import doobie._
import eventstore.EventRepository
import eventstore.EventRepository.Error.Unexpected
import eventstore.RepositoryEvent
import eventstore.RepositoryWriteEvent
import eventstore.types.AggregateName
import eventstore.types.EventStoreVersion
import eventstore.types.EventStreamId
import play.api.libs.json.Json
import play.api.libs.json.Reads
import play.api.libs.json.Writes
import zio.ZIO
import zio._
import zio.stream.Stream
import zio.stream.ZStream

import EventRepository.{Direction, Error, Subscription}

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

  override def getEventStream[EventType: Reads: Tag](
      eventStreamId: EventStreamId,
      direction: Direction = Direction.Forward
  ): ZIO[Scope, Unexpected, Stream[Unexpected, RepositoryEvent[EventType]]] = {
    implicit val getEventType: Get[EventType] = getJson[EventType]
    postgresEventRepositoryLive.getEventStream[EventType](eventStreamId, direction)
  }

  override def saveEvents[EventType: Reads: Writes: Tag](
      eventStreamId: EventStreamId,
      events: Seq[RepositoryWriteEvent[EventType]]
  ): IO[EventRepository.SaveEventError, Seq[RepositoryEvent[EventType]]] = {
    implicit val getEventType: Get[EventType] = getJson[EventType]
    implicit val putEventType: Put[EventType] = putJson[EventType]
    postgresEventRepositoryLive.saveEvents(eventStreamId, events)
  }

  override def listen[EventType: Reads: Tag]: ZIO[
    Scope,
    Error.Unexpected,
    EventRepository.Subscription[EventType]
  ] = {
    implicit val getEventType: Get[EventType] = getJson[EventType]
    postgresEventRepositoryLive.listen[EventType]

  }

  override def listenFromVersion[EventType: Reads: Tag](
      fromExclusive: EventStoreVersion
  ): ZIO[Scope, Unexpected, Subscription[EventType]] = {
    implicit val getEventType: Get[EventType] = getJson[EventType]
    postgresEventRepositoryLive.listenFromVersion[EventType](fromExclusive)
  }

  override def getAllEvents[A: Reads: Tag]: ZIO[Scope, Nothing, Stream[Unexpected, RepositoryEvent[A]]] = {
    implicit val getEventType: Get[A] = getJson[A]
    postgresEventRepositoryLive.getAllEvents[A]
  }

  override def getEventByStoreVersion[A: Reads: Tag](
      version: EventStoreVersion
  ): IO[Unexpected, Option[RepositoryEvent[A]]] = {
    implicit val getEventType: Get[A] = getJson[A]
    postgresEventRepositoryLive.getEventByStoreVersion[A](version)
  }

  override def listEventStreamWithName(
      aggregateName: AggregateName,
      direction: Direction = Direction.Forward
  ): ZStream[Any, Error.Unexpected, EventStreamId] =
    postgresEventRepositoryLive.listEventStreamWithName(aggregateName, direction)

}

object PostgresPlayJsonEventRepository {
  def layer: URLayer[EventRepository[Get, Put], EventRepository[Reads, Writes]] =
    ZLayer.derive[PostgresPlayJsonEventRepository]
}
