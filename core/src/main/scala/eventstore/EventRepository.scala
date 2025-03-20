package eventstore

import eventstore.EventRepository.Direction
import eventstore.EventRepository.Error.Unexpected
import eventstore.EventRepository.SaveEventError
import eventstore.EventRepository.Subscription
import eventstore.SwitchableZStream.Message
import eventstore.types.AggregateName
import eventstore.types.AggregateVersion
import eventstore.types.EventStoreVersion
import eventstore.types.EventStreamId
import zio.IO
import zio.Scope
import zio.Tag
import zio.ZIO
import zio.stream.Stream
import zio.stream.ZStream

object EventRepository {

  sealed trait Direction
  object Direction {
    case object Forward extends Direction
    case object Backward extends Direction
  }

  sealed trait LastEventToHandle
  object LastEventToHandle {
    case class Version(version: EventStoreVersion) extends LastEventToHandle
    case object LastEvent extends LastEventToHandle
  }

  trait Subscription[EventType, DoneBy] {
    def restartFromFirstEvent(lastEventToHandle: LastEventToHandle = LastEventToHandle.LastEvent): IO[Unexpected, Unit]
    def stream: ZStream[Scope, Unexpected, EventStoreEvent[EventType, DoneBy]]
  }

  object Subscription {
    def fromSwitchableStream[EventType, DoneBy](
        switchableStream: SwitchableZStream[Any, Unexpected, RepositoryEvent[EventType, DoneBy]],
        maybeLastVersion: IO[Unexpected, Option[EventStoreVersion]]
    ): Subscription[EventType, DoneBy] =
      new Subscription[EventType, DoneBy] {

        override def restartFromFirstEvent(
            lastEventToHandle: LastEventToHandle = LastEventToHandle.LastEvent
        ): IO[Unexpected, Unit] =
          lastEventToHandle match {
            case LastEventToHandle.Version(version) =>
              switchableStream.switchToPastEvents(_.eventStoreVersion == version)
            case LastEventToHandle.LastEvent =>
              maybeLastVersion.some
                .flatMap(version => switchableStream.switchToPastEvents(_.eventStoreVersion == version))
                .unsome
                .someOrElseZIO(switchableStream.switchToEmptyPastEvents)
          }

        override def stream: ZStream[Scope, Unexpected, EventStoreEvent[EventType, DoneBy]] =
          switchableStream.stream.collect {
            case Message.SwitchedToPastEvents => Reset[EventType, DoneBy]()
            case Message.Event(a)             => a
          }

      }

  }

  sealed trait SaveEventError

  sealed trait Error
  object Error {
    case class VersionConflict(provided: AggregateVersion, required: AggregateVersion) extends Error with SaveEventError
    case class Unexpected(throwable: Throwable) extends Error with SaveEventError
  }

  implicit class EventsOps[E, DoneBy](self: Seq[RepositoryWriteEvent[E, DoneBy]]) {

    def checkVersionsAreContiguousIncrements: Either[Unexpected, Unit] = self match {
      case _ :: tail =>
        self.zip(tail).foldLeft[Either[Unexpected, Unit]](Right[Unexpected, Unit](())) {
          case (invalid @ Left(_), _) => invalid
          case (valid, (current, next)) =>
            if (current.aggregateVersion.next == next.aggregateVersion) valid
            else
              Left(
                Unexpected(
                  new IllegalArgumentException(
                    s"Invalid version sequence current: ${current.aggregateVersion}, next: ${next.aggregateVersion}"
                  )
                )
              )
        }
      case _ => Right(())
    }
  }
}

trait EventRepository[Decoder[_], Encoder[_]] {

  def getAllEvents[A: Decoder: Tag, DoneBy: Decoder: Tag]
      : ZIO[Scope, Nothing, Stream[Unexpected, RepositoryEvent[A, DoneBy]]]

  def listEventStreamWithName(
      aggregateName: AggregateName,
      direction: Direction = Direction.Forward
  ): Stream[Unexpected, EventStreamId]

  def getEventStream[A: Decoder: Tag, DoneBy: Decoder: Tag](
      eventStreamId: EventStreamId
  ): IO[Unexpected, Seq[RepositoryEvent[A, DoneBy]]]

  def saveEvents[A: Decoder: Encoder: Tag, DoneBy: Decoder: Encoder: Tag](
      eventStreamId: EventStreamId,
      events: Seq[RepositoryWriteEvent[A, DoneBy]]
  ): IO[SaveEventError, Seq[RepositoryEvent[A, DoneBy]]]

  def listen[EventType: Decoder: Tag, DoneBy: Decoder: Tag]: ZIO[Scope, Unexpected, Subscription[EventType, DoneBy]]

}
