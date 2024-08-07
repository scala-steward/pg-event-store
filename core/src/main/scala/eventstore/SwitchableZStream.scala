package eventstore

import eventstore.SwitchableZStream.Message
import eventstore.SwitchableZStream.StreamState
import zio.Dequeue
import zio.Ref
import zio.Scope
import zio.UIO
import zio.ZIO
import zio.stream.Take
import zio.stream.ZStream

private[eventstore] class SwitchableZStream[-R, +E, +A] private (
    stream1: ZStream[R, E, A],
    stream2: UIO[ZStream[R, E, A]],
    stateRef: Ref[StreamState[A]]
) {
  def stream: ZStream[R & Scope, E, Message[A]] =
    ZStream.unwrap {
      for {
        queue2Ref <- ZStream.empty
          .toQueue()
          .flatMap(queue => Ref.make[Dequeue[Take[E, Message.Event[A]]]](queue))
        queue1 <- stream1.map(Message.Event(_)).toQueue()
      } yield {
        val queue2Factory = stream2.flatMap(_.map(Message.Event(_)).toQueue())
        def replaceQueue2 =
          queue2Factory.flatMap(queue2Ref.getAndSet).flatMap(_.shutdown)

        ZStream
          .repeatZIO {

            for {
              state <- stateRef.get
              message <- state match {
                case StreamState.First        => queue1.poll
                case _: StreamState.Second[A] => queue2Ref.get.flatMap(_.poll)
                case StreamState.SwitchingToFirst =>
                  stateRef
                    .set(StreamState.First)
                    .as(Take.single(Message.SwitchedToFirst))
                    .asSome
                case switch: StreamState.SwitchingToSecond[A] =>
                  replaceQueue2 *>
                    stateRef
                      .set(StreamState.Second(switch.condition))
                      .as(Take.single(Message.SwitchedToSecond))
                      .asSome
                case StreamState.NotStarted =>
                  stateRef.set(StreamState.First) *> queue1.poll
                case start: StreamState.StartWithSecond[A] =>
                  replaceQueue2 *>
                    stateRef.set(
                      StreamState.Second(start.condition)
                    ) *> queue2Ref.get.flatMap(_.poll)
              }
            } yield message
          }
          .collectSome
          .flattenTake
          .tap { message =>
            stateRef.get.map(_ -> message).flatMap {
              case (second: StreamState.Second[A], Message.Event(a)) =>
                ZIO.when(second.condition(a))(switchToFirst)
              case _ => ZIO.unit
            }
          }
      }
    }

  def switchToFirst: UIO[Unit] = stateRef.update {
    case StreamState.NotStarted              => StreamState.First
    case StreamState.First                   => StreamState.First
    case _: StreamState.Second[A]            => StreamState.SwitchingToFirst
    case StreamState.SwitchingToFirst        => StreamState.SwitchingToFirst
    case _: StreamState.SwitchingToSecond[A] => StreamState.SwitchingToFirst
    case _: StreamState.StartWithSecond[A]   => StreamState.SwitchingToFirst
  }
  def switchToSecond: UIO[Unit] = switchToSecond(condition = (_: A) => false)

  /** This method allows to switch from first stream to second stream and switch back to first stream automatically once
    * the condition is met.
    */
  def switchToSecondUntil(condition: A => Boolean): UIO[Unit] = switchToSecond(
    condition
  )

  private def switchToSecond(condition: A => Boolean): UIO[Unit] =
    stateRef.update {
      case StreamState.NotStarted              => StreamState.StartWithSecond(condition)
      case _: StreamState.Second[A]            => StreamState.Second(condition)
      case StreamState.First                   => StreamState.SwitchingToSecond(condition)
      case StreamState.SwitchingToFirst        => StreamState.SwitchingToSecond(condition)
      case _: StreamState.SwitchingToSecond[A] => StreamState.SwitchingToSecond(condition)
      case _: StreamState.StartWithSecond[A]   => StreamState.StartWithSecond(condition)
    }

}

private[eventstore] object SwitchableZStream {

  private sealed trait StreamState[+A]
  private object StreamState {
    case object NotStarted extends StreamState[Nothing]
    case object First extends StreamState[Nothing]
    case object SwitchingToFirst extends StreamState[Nothing]
    case class Second[A](condition: A => Boolean) extends StreamState[A]
    case class StartWithSecond[A](condition: A => Boolean) extends StreamState[A]
    case class SwitchingToSecond[A](condition: A => Boolean) extends StreamState[A]
  }

  sealed trait Message[+A]
  object Message {
    case object SwitchedToFirst extends Message[Nothing]
    case object SwitchedToSecond extends Message[Nothing]
    case class Event[A](a: A) extends Message[A]
  }

  def from[R, E, A](
      stream1: ZStream[R, E, A],
      stream2: UIO[ZStream[R, E, A]]
  ): UIO[SwitchableZStream[R, E, A]] =
    for {
      stateRef <- Ref.make[StreamState[A]](StreamState.NotStarted)
    }
    // TODO: instead of rechunk, we could prevent excess records from being sent downstream with a simple filter
    yield new SwitchableZStream(
      stream1.rechunk(1),
      stream2.map(_.rechunk(1)),
      stateRef
    )
}
