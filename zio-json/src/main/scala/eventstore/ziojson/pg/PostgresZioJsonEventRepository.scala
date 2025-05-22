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
    Get[String].temap(input =>
      implicitly[JsonDecoder[A]]
        .decodeJson(input)
        .left
        .map(msg => s"trying to decode $input as ${implicitly[Tag[A]].tag}, error $msg")
    )

  def putJson[A](implicit encoder: JsonEncoder[A]): Put[A] =
    Put[String].contramap(obj => encoder.encodeJson(obj, indent = None).toString)

  override def getEventStream[A: JsonDecoder: Tag, DoneBy: JsonDecoder: Tag](
      eventStreamId: EventStreamId
  ): IO[Unexpected, Seq[RepositoryEvent[A, DoneBy]]] = {
    implicit val getA: Get[A] = getJson[A]
    implicit val getDoneBy: Get[DoneBy] = getJson[DoneBy]
    postgresEventRepositoryLive.getEventStream[A, DoneBy](eventStreamId)
  }

  override def saveEvents[A: JsonDecoder: JsonEncoder: Tag, DoneBy: JsonDecoder: JsonEncoder: Tag](
      eventStreamId: EventStreamId,
      events: Seq[RepositoryWriteEvent[A, DoneBy]]
  ): IO[EventRepository.SaveEventError, Seq[RepositoryEvent[A, DoneBy]]] = {
    implicit val getA: Get[A] = getJson[A]
    implicit val putA: Put[A] = putJson[A]
    implicit val getDoneBy: Get[DoneBy] = getJson[DoneBy]
    implicit val putDoneBy: Put[DoneBy] = putJson[DoneBy]
    postgresEventRepositoryLive.saveEvents(eventStreamId, events)
  }

  override def listen[EventType: JsonDecoder: Tag, DoneBy: JsonDecoder: Tag]: ZIO[
    Scope,
    Unexpected,
    EventRepository.Subscription[EventType, DoneBy]
  ] = {
    implicit val getEventType: Get[EventType] = getJson[EventType]
    implicit val getDoneBy: Get[DoneBy] = getJson[DoneBy]
    postgresEventRepositoryLive.listen[EventType, DoneBy]

  }

  override def listenFromVersion[EventType: JsonDecoder: Tag, DoneBy: JsonDecoder: Tag](
      fromExclusive: EventStoreVersion
  ): ZIO[Scope, Unexpected, Subscription[EventType, DoneBy]] = {
    implicit val getEventType: Get[EventType] = getJson[EventType]
    implicit val getDoneBy: Get[DoneBy] = getJson[DoneBy]
    postgresEventRepositoryLive.listenFromVersion[EventType, DoneBy](fromExclusive)
  }

  override def getAllEvents[A: JsonDecoder: Tag, DoneBy: JsonDecoder: Tag]
      : ZIO[Scope, Nothing, Stream[Unexpected, RepositoryEvent[A, DoneBy]]] = {
    implicit val getA: Get[A] = getJson[A]
    implicit val getDoneBy: Get[DoneBy] = getJson[DoneBy]
    postgresEventRepositoryLive.getAllEvents[A, DoneBy]
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
