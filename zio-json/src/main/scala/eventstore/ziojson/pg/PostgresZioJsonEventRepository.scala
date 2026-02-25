package eventstore.ziojson.pg

import doobie.Get
import doobie.Put
import eventstore.EventRepository
import eventstore.EventRepository.Direction
import eventstore.EventRepository.Error.Unexpected
import eventstore.EventRepository.Subscription
import eventstore.RepositoryEvent
import eventstore.RepositoryWriteEvent
import eventstore.types.AggregateName
import eventstore.types.EventStoreVersion
import eventstore.types.EventStreamId
import zio._
import zio.json._
import zio.stream.Stream
import zio.stream.ZStream

private class PostgresZioJsonEventRepository(
    postgresEventRepositoryLive: EventRepository[Get, Put]
) extends EventRepository[JsonDecoder, JsonEncoder] {

  def getJson[A: JsonDecoder: Tag]: Get[A] =
    Get[String].temap(input => {
      implicitly[JsonDecoder[A]]
        .decodeJson(input)
        .left
        .map(msg => s"trying to decode $input as ${implicitly[Tag[A]].tag}, error $msg")
    })

  def putJson[A](implicit encoder: JsonEncoder[A]): Put[A] =
    Put[String].contramap(obj => encoder.encodeJson(obj, indent = None).toString)

  override def getEventStream[A: JsonDecoder: Tag](
      eventStreamId: EventStreamId,
      direction: Direction = Direction.Forward
  ): ZIO[Scope, Unexpected, Stream[Unexpected, RepositoryEvent[A]]] = {
    implicit val getA: Get[A] = getJson[A]
    postgresEventRepositoryLive.getEventStream[A](eventStreamId, direction)
  }

  override def saveEvents[A: JsonDecoder: JsonEncoder: Tag](
      eventStreamId: EventStreamId,
      events: Seq[RepositoryWriteEvent[A]]
  ): IO[EventRepository.SaveEventError, Seq[RepositoryEvent[A]]] = {
    implicit val getA: Get[A] = getJson[A]
    implicit val putA: Put[A] = putJson[A]
    postgresEventRepositoryLive.saveEvents(eventStreamId, events)
  }

  override def listen[EventType: JsonDecoder: Tag]: ZIO[
    Scope,
    Unexpected,
    EventRepository.Subscription[EventType]
  ] = {
    implicit val getEventType: Get[EventType] = getJson[EventType]
    postgresEventRepositoryLive.listen[EventType]

  }

  override def listenFromVersion[EventType: JsonDecoder: Tag](
      fromExclusive: EventStoreVersion
  ): ZIO[Scope, Unexpected, Subscription[EventType]] = {
    implicit val getEventType: Get[EventType] = getJson[EventType]
    postgresEventRepositoryLive.listenFromVersion[EventType](fromExclusive)
  }

  override def getAllEvents[A: JsonDecoder: Tag]: ZIO[Scope, Nothing, Stream[Unexpected, RepositoryEvent[A]]] = {
    implicit val getA: Get[A] = getJson[A]
    postgresEventRepositoryLive.getAllEvents[A]
  }

  override def getEventByStoreVersion[A: JsonDecoder: Tag](
      version: EventStoreVersion
  ): IO[Unexpected, Option[RepositoryEvent[A]]] = {
    implicit val getA: Get[A] = getJson[A]
    postgresEventRepositoryLive.getEventByStoreVersion[A](version)
  }

  override def listEventStreamWithName(
      aggregateName: AggregateName,
      direction: Direction = Direction.Forward
  ): ZStream[Any, Unexpected, EventStreamId] =
    postgresEventRepositoryLive.listEventStreamWithName(aggregateName, direction)
}

object PostgresZioJsonEventRepository {

  def layer: URLayer[EventRepository[Get, Put], EventRepository[JsonDecoder, JsonEncoder]] =
    ZLayer.derive[PostgresZioJsonEventRepository]
}
