package eventstore.pg

import cats.data.EitherT
import cats.implicits.catsSyntaxApplicativeId
import doobie._
import doobie.enumerated.TransactionIsolation
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.update.Update
import eventstore.EventRepository
import eventstore.EventRepository.Direction
import eventstore.EventRepository.Error.Unexpected
import eventstore.EventRepository.Error.VersionConflict
import eventstore.EventRepository.EventsOps
import eventstore.EventRepository.SaveEventError
import eventstore.EventRepository.Subscription
import eventstore.RepositoryEvent
import eventstore.RepositoryWriteEvent
import eventstore.SwitchableZStream
import eventstore.pg.Postgres.ZTransactor
import eventstore.types.AggregateId
import eventstore.types.AggregateName
import eventstore.types.AggregateVersion
import eventstore.types.EventStoreVersion
import eventstore.types.EventStreamId
import eventstore.types.ProcessId
import zio._
import zio.interop.catz._
import zio.stream.Stream
import zio.stream.Take
import zio.stream.ZStream
import zio.stream.interop.fs2z._

import java.sql.Savepoint
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

class PostgresEventRepositoryLive(
    transactor: ZTransactor,
    hub: Hub[RepositoryEvent[Any, Any]]
) extends EventRepository[Get, Put] {

  import Codecs.*

  def getEventStream[A: Get: Tag, DoneBy: Get: Tag](
      eventStreamId: EventStreamId,
      direction: Direction = Direction.Forward
  ): ZIO[Scope, Unexpected, Stream[Unexpected, RepositoryEvent[A, DoneBy]]] =
    ZIO.succeed {
      fs2RIOStreamSyntax(
        Req
          .list(eventStreamId, direction)
          .query[RepositoryEvent[A, DoneBy]]
          .stream
          .transact(transactor)
      )
        .toZStream()
        .tapErrorCause(ZIO.logErrorCause("getEventStream", _))
        .mapError(Unexpected.apply)
    }

  override def saveEvents[E: Get: Put: Tag, DoneBy: Get: Put: Tag](
      eventStreamId: EventStreamId,
      writeEvents: Seq[RepositoryWriteEvent[E, DoneBy]]
  ): IO[SaveEventError, Seq[RepositoryEvent[E, DoneBy]]] = {
    for {
      _ <- ZIO.fromEither(writeEvents.checkVersionsAreContiguousIncrements)

      events <- transactionalSave(eventStreamId, writeEvents)

      _ <- hub.publishAll(events)
    } yield events
  }

  override def listen[EventType: Get: Tag, DoneBy: Get: Tag]: ZIO[Scope, Unexpected, Subscription[EventType, DoneBy]] =
    for {
      live <- fromHub[EventType, DoneBy]
      fromDb = getAllEvents[EventType, DoneBy]
      switchableStream <- SwitchableZStream.from(live, fromDb)
    } yield Subscription.fromSwitchableStream(switchableStream, getLastEventVersion)

  private def fromHub[EventType: Tag, DoneBy: Tag]
      : ZIO[Scope, Nothing, ZStream[Any, Nothing, RepositoryEvent[EventType, DoneBy]]] =
    for {
      subscription <- hub.subscribe
    } yield {
      val eventTypeTag = implicitly[Tag[EventType]].tag
      val doneByTag = implicitly[Tag[DoneBy]].tag

      ZStream
        .fromQueue(subscription)
        .collect {
          case event: RepositoryEvent[Any, Any] if event.eventTag <:< eventTypeTag && event.doneByTag <:< doneByTag =>
            event.asInstanceOf[RepositoryEvent[EventType, DoneBy]]
        }
    }

  override def listenFromVersion[EventType: Get: Tag, DoneBy: Get: Tag](
      fromExclusive: EventStoreVersion
  ): ZIO[Scope, Unexpected, Subscription[EventType, DoneBy]] = {
    val fromDb = getAllEvents[EventType, DoneBy]

    for {
      fromVersion <- getAllEventImpl[EventType, DoneBy](query = Req.listAllFromVersion(fromExclusive))
      live <- fromHub[EventType, DoneBy]
      switchableStream <- SwitchableZStream.from(fromVersion.concat(live), fromDb)
    } yield Subscription.fromSwitchableStream(switchableStream, getLastEventVersion)
  }

  override def getAllEvents[A: Get: Tag, DoneBy: Get: Tag]
      : ZIO[Scope, Nothing, Stream[Unexpected, RepositoryEvent[A, DoneBy]]] =
    getAllEventImpl[A, DoneBy](Req.listAll)

  private def getAllEventImpl[A: Get: Tag, DoneBy: Get: Tag](query: Fragment) =
    for {
      queue <- ZIO.acquireRelease(Queue.bounded[Take[Unexpected, RepositoryEvent[A, DoneBy]]](16))(_.shutdown)
      _ <- query
        .query[RepositoryEvent[A, DoneBy]]
        .stream
        .transact(transactor)
        .toZStream()
        .mapError(Unexpected.apply)
        .runIntoQueue(queue)
        .tapErrorCause(ZIO.logErrorCause("getAllEvents", _))
        .forkScoped // to not block the flow if the number of events to insert is superior to the queue capacity
    } yield ZStream.fromQueue(queue).flattenTake

  override def listEventStreamWithName(
      aggregateName: AggregateName,
      direction: Direction = Direction.Forward
  ): Stream[Unexpected, EventStreamId] =
    Req
      .listStreams(aggregateName, direction)
      .query[EventStreamId]
      .stream
      .transact(transactor)
      .toZStream()
      .tapErrorCause(ZIO.logErrorCause("listEventStreamWithName", _))
      .mapError(Unexpected.apply)

  private def transactionalSave[E: Get: Put: Tag, DoneBy: Get: Put: Tag](
      eventStreamId: EventStreamId,
      writeEvents: Seq[RepositoryWriteEvent[E, DoneBy]]
  ): IO[SaveEventError, List[RepositoryEvent[E, DoneBy]]] =
    (for {
      _ <- setTransactionIsolation
      expectedVersion <- queryMaxVersionForAggregate(eventStreamId)
      _ <- checkExpectedVersion(expectedVersion, writeEvents)
      events <- insertEvents(eventStreamId, writeEvents)
    } yield events).value
      .transact(transactor)
      .orDie
      .absolve

  private def setTransactionIsolation: EitherT[ConnectionIO, SaveEventError, Unit] =
    EitherT.right {
      for {
        _ <- FC.setAutoCommit(false)
        _ <- FC.setTransactionIsolation(TransactionIsolation.TransactionReadCommitted.toInt)
      } yield ()
    }

  private def queryMaxVersionForAggregate(eventStreamId: EventStreamId) =
    EitherT {
      Req
        .selectMaxVersion(eventStreamId)
        .query[AggregateVersion]
        .unique
        .attemptSql
    }
      .leftMap[SaveEventError](Unexpected.apply)

  private def getLastEventVersion: IO[Unexpected, Option[EventStoreVersion]] =
    Req.selectLastEventStoreVersion
      .query[Option[EventStoreVersion]]
      .unique
      .transact(transactor)
      .mapError { Unexpected.apply }

  private def insertEvents[DoneBy: Get: Put: Tag, E: Get: Put: Tag](
      eventStreamId: EventStreamId,
      writeEvents: Seq[RepositoryWriteEvent[E, DoneBy]]
  ): EitherT[ConnectionIO, SaveEventError, List[RepositoryEvent[E, DoneBy]]] = for {
    savepoint <- EitherT.right { FC.setSavepoint }

    result <- EitherT {
      Req
        .insert[E, DoneBy]
        .updateManyWithGeneratedKeys[RepositoryEvent[E, DoneBy]](
          "processid",
          "aggregateid",
          "aggregatename",
          "sentdate",
          "payload",
          "doneBy",
          "aggregateVersion",
          "eventStoreVersion"
        )(writeEvents)
        .compile
        .toList
        .map[Either[SaveEventError, List[RepositoryEvent[E, DoneBy]]]](Right.apply)
        .onUniqueViolation {
          generateVersionConflictError(
            eventStreamId = eventStreamId,
            savepoint = savepoint,
            providedVersion = writeEvents.head.aggregateVersion
          ).value
        }
        .attemptSql
    }
      .leftMap { Unexpected(_) }
      .flatMap { EitherT.fromEither(_) }
  } yield result

  private def generateVersionConflictError[A](
      eventStreamId: EventStreamId,
      savepoint: Savepoint,
      providedVersion: AggregateVersion
  ): EitherT[ConnectionIO, SaveEventError, A] =
    for {
      _ <- EitherT.right { FC.rollback(savepoint) }
      expected <- queryMaxVersionForAggregate(eventStreamId)
      result <- EitherT.left[A][ConnectionIO, SaveEventError](
        (VersionConflict(provided = providedVersion, required = expected): SaveEventError).pure[ConnectionIO]
      )
    } yield result

  private def checkExpectedVersion(
      expectedVersion: AggregateVersion,
      newEvents: Seq[RepositoryWriteEvent[?, ?]]
  ): EitherT[ConnectionIO, SaveEventError, Unit] =
    EitherT.fromEither {
      newEvents.headOption
        .map { headEvent =>
          val headVersion = headEvent.aggregateVersion
          if (headVersion == expectedVersion) {
            Right(())
          } else {
            Left(VersionConflict(headVersion, expectedVersion))
          }
        }
        .getOrElse(Right(()))
    }
}

object Codecs {
  implicit val aggregateVersionPut: Put[AggregateVersion] = implicitly[Put[Int]].contramap(_.asInt)
  implicit val aggregateVersionGet: Get[AggregateVersion] = implicitly[Get[Int]].map(AggregateVersion.apply)

  implicit val eventStoreVersionPut: Put[EventStoreVersion] = implicitly[Put[Int]].contramap(_.asInt)
  implicit val eventStoreVersionGet: Get[EventStoreVersion] = implicitly[Get[Int]].map(EventStoreVersion.apply)

  implicit val processIdPut: Put[ProcessId] = implicitly[Put[UUID]].contramap(_.asUuid)
  implicit val processIdGet: Get[ProcessId] = implicitly[Get[UUID]].map(ProcessId.apply)

  implicit val aggregateIdPut: Put[AggregateId] = implicitly[Put[UUID]].contramap(_.asUuid)
  implicit val aggregateIdGet: Get[AggregateId] = implicitly[Get[UUID]].map(AggregateId.apply)

  implicit val aggregateNamePut: Put[AggregateName] = implicitly[Put[String]].contramap(_.asString)
  implicit val aggregateNameGet: Get[AggregateName] = implicitly[Get[String]].map(AggregateName.apply)

  implicit val offsetDateTimeGet: Get[OffsetDateTime] =
    Get[String].map(date => OffsetDateTime.parse(date, DateTimeFormatter.ISO_DATE_TIME))

  implicit val offsetDateTimePut: Put[OffsetDateTime] =
    Put[String].contramap(_.format(DateTimeFormatter.ISO_DATE_TIME))

  implicit def repositoryEventRead[E: Get: Tag, DoneBy: Get: Tag]: Read[RepositoryEvent[E, DoneBy]] =
    Read[(ProcessId, AggregateId, AggregateName, AggregateVersion, OffsetDateTime, EventStoreVersion, DoneBy, E)]
      .map { x => RepositoryEvent(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8) }

  implicit def repositoryWriteEventWrite[E: Put, DoneBy: Put]: Write[RepositoryWriteEvent[E, DoneBy]] =
    Write[(ProcessId, AggregateId, AggregateName, AggregateVersion, OffsetDateTime, DoneBy, E)]
      .contramap[RepositoryWriteEvent[E, DoneBy]] { x =>
        (x.processId, x.aggregateId, x.aggregateName, x.aggregateVersion, x.sentDate, x.doneBy, x.event)
      }
}

object PostgresEventRepositoryLive {
  def layer: URLayer[ZTransactor, EventRepository[Get, Put]] = ZLayer.derive[PostgresEventRepositoryLive]
}

private object Req {
  import Codecs.*

  def list(eventStreamId: EventStreamId, direction: Direction): Fragment = {
    val order = direction match {
      case Direction.Backward => fr"desc"
      case Direction.Forward  => fr"asc"
    }

    val aggId = eventStreamId.aggregateId
    val aggName = eventStreamId.aggregateName

    sql"""select processid, aggregateid, aggregatename, aggregateVersion, sentdate, eventStoreVersion, doneBy, payload
		  from events
		  where aggregatename=$aggName
		  and aggregateid=$aggId
		  order by aggregateVersion """ ++ order
  }

  def listAll: Fragment =
    selectEvent(whereOpt = None)

  def listAllFromVersion(eventStoreVersion: EventStoreVersion): Fragment =
    selectEvent(whereOpt = Some(sql"""eventStoreVersion > $eventStoreVersion"""))

  private def selectEvent(whereOpt: Option[Fragment]): Fragment =
    sql"""select processid, aggregateid, aggregatename, aggregateVersion, sentdate, eventStoreVersion, doneBy, payload
		  from events """ ++ Fragments.whereAndOpt(whereOpt) ++ Fragments.orderBy(sql"""eventStoreVersion""")

  def listStreams(aggregateName: AggregateName, direction: Direction): Fragment = {
    val order = direction match {
      case Direction.Backward => fr"desc"
      case Direction.Forward  => fr"asc"
    }
    sql"""with aggregateWithMinVersion as (
            select aggregateid, aggregatename, min(eventStoreVersion) as version
   		    from events
   		    where aggregatename=$aggregateName
   		    group by (aggregateid, aggregatename) 
   		    order by version $order)
          select aggregateid, aggregatename from aggregateWithMinVersion"""
  }

  def insert[A: Put, DoneBy: Put]: Update[RepositoryWriteEvent[A, DoneBy]] =
    Update[RepositoryWriteEvent[A, DoneBy]](
      """
              insert into events
                (processid, aggregateid, aggregatename, aggregateVersion, sentdate, doneBy, payload)
              values
                (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)
			  returning
                processid, aggregateid, aggregatename, aggregateVersion, sentdate, eventStoreVersion, doneBy, payload
     		"""
    )

  def selectMaxVersion(eventStreamId: EventStreamId) = {
    sql"""select coalesce(max(aggregateVersion) + 1, ${AggregateVersion.initial})
          from events
          where aggregateid=${eventStreamId.aggregateId}
          and aggregatename=${eventStreamId.aggregateName}"""
  }

  def selectLastEventStoreVersion = {
    sql"""select max(eventStoreVersion) from events"""
  }
}
