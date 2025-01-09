package eventstore

import eventstore.SwitchableZStream.Command
import eventstore.SwitchableZStream.CommandOrEvent
import eventstore.SwitchableZStream.Message
import eventstore.SwitchableZStream.StreamState
import zio.Dequeue
import zio.IO
import zio.Queue
import zio.Ref
import zio.Scope
import zio.UIO
import zio.ZIO
import zio.stream.Take
import zio.stream.ZStream

private[eventstore] class SwitchableZStream[-R, +E, +A] private (
    liveStream: ZStream[R, E, A],
    loadPastEvents: UIO[ZStream[R, E, A]],
    stateRef: Ref[StreamState[E, A]],
    commands: Queue[Command[A]]
) {
  def stream: ZStream[R & Scope, E, Message[A]] =
    ZStream.unwrap {
      for {
        liveQueue <- liveStream.map(Message.Event(_)).toQueue()

      } yield {

        def setState(state: StreamState[E, A]) =
          for {
            previousState <- stateRef.getAndSet(state)
            _ <- previousState match {
              case s: StreamState.PastEvents[_, _] => s.pastEventQueue.shutdown
              case StreamState.NotStarted          => ZIO.unit
              case StreamState.Live                => ZIO.unit
            }
          } yield state

        def selectRunningQueue(state: StreamState[E, A]) = state match {
          case StreamState.Live                => liveQueue
          case s: StreamState.PastEvents[E, A] => s.pastEventQueue
          case StreamState.NotStarted          => liveQueue
        }

        def commandOrEvent(
            commands: Dequeue[Command[A]],
            events: Dequeue[Take[E, Message.Event[A]]]
        ): IO[E, CommandOrEvent[A]] = {
          // we need commands to have priority over events so we start
          // by polling the commands queue then we race events and commands
          commands.poll.some
            .map(CommandOrEvent.Command(_))
            .unsome
            .someOrElseZIO(
              events.takeAsEvent
                .raceEither(commands.take)
                .map {
                  case Left(event)    => CommandOrEvent.Event(event)
                  case Right(command) => CommandOrEvent.Command(command)
                }
            )
        }

        def handle(commandOrEvent: CommandOrEvent[A]) = commandOrEvent match {
          case CommandOrEvent.Command(Command.SwitchToLive) =>
            setState(StreamState.Live).as(Message.SwitchedToLive)

          case CommandOrEvent.Command(c: Command.SwitchToPastEvents[A]) =>
            for {
              pastEvents <- loadPastEvents.flatMap(_.map(Message.Event(_)).toQueue())
              _ <- setState(StreamState.PastEvents(pastEventQueue = pastEvents, until = c.until))
            } yield Message.SwitchedToPastEvents

          case CommandOrEvent.Event(event) =>
            for {
              state <- stateRef.get
              _ <- (state, event) match {
                case (pastEvents: StreamState.PastEvents[E, A], Message.Event(a)) =>
                  ZIO.when(pastEvents.until(a))(switchToLive)
                case _ => ZIO.unit
              }
            } yield event
        }

        ZStream
          .repeatZIO {
            for {
              state <- stateRef.get
              runningQueue = selectRunningQueue(state)
              commandOrEvent <- commandOrEvent(commands, runningQueue)
              message <- handle(commandOrEvent)
            } yield message
          }
      }
    }

  def switchToLive: UIO[Unit] = commands.offer(Command.SwitchToLive).unit
  def switchToPastEvents: UIO[Unit] = switchToPastEvents(until = (_: A) => false)

  /** This method allows to switch from live stream to past events stream and switch back to live stream automatically
    * once the condition is met.
    */
  def switchToPastEvents(until: A => Boolean): UIO[Unit] =
    commands.offer(Command.SwitchToPastEvents(until)).unit

  implicit class QueueOps[E1, B](queue: Dequeue[Take[E1, Message.Event[B]]]) {

    def takeAsEvent: IO[E1, Message.Event[B]] =
      queue.take.flatMap(
        _.foldZIO(
          end = ZIO.dieMessage("unexpected end of stream"),
          error = cause => ZIO.failCause(cause),
          value = chunk => ZIO.fromOption(chunk.headOption).orElse(ZIO.dieMessage("take didn't return any parameter"))
        )
      )
  }

}

private[eventstore] object SwitchableZStream {

  private sealed trait Command[+A]
  private object Command {
    case object SwitchToLive extends Command[Nothing]
    case class SwitchToPastEvents[A](until: A => Boolean) extends Command[A]
  }

  private sealed trait CommandOrEvent[+A]
  private object CommandOrEvent {
    case class Command[A](command: SwitchableZStream.Command[A]) extends CommandOrEvent[A]
    case class Event[E, A](event: Message.Event[A]) extends CommandOrEvent[A]
  }

  private sealed trait StreamState[+E, +A]
  private object StreamState {
    case object NotStarted extends StreamState[Nothing, Nothing]
    case object Live extends StreamState[Nothing, Nothing]
    case class PastEvents[E, A](pastEventQueue: Dequeue[Take[E, Message.Event[A]]], until: A => Boolean)
        extends StreamState[E, A]
  }

  sealed trait Message[+A]
  object Message {
    case object SwitchedToLive extends Message[Nothing]
    case object SwitchedToPastEvents extends Message[Nothing]
    case class Event[A](a: A) extends Message[A]
  }

  def from[R, E, A](
      stream1: ZStream[R, E, A],
      stream2: UIO[ZStream[R, E, A]]
  ): UIO[SwitchableZStream[R, E, A]] =
    for {
      stateRef <- Ref.make[StreamState[E, A]](StreamState.NotStarted)
      commands <- Queue.unbounded[Command[A]]
    }
    // TODO: instead of rechunk, we could prevent excess records from being sent downstream with a simple filter
    yield new SwitchableZStream(
      liveStream = stream1.rechunk(1),
      loadPastEvents = stream2.map(_.rechunk(1)),
      stateRef = stateRef,
      commands = commands
    )

}
