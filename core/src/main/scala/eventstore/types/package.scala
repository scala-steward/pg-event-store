package eventstore

import zio.Random
import zio.UIO

import java.util.UUID
import scala.util.Try

package object types {

  case class AggregateVersion private[eventstore] (asInt: Int) extends AnyVal {
    def next: AggregateVersion = AggregateVersion(asInt + 1)
  }

  object AggregateVersion {
    val initial: AggregateVersion = AggregateVersion(0)
  }

  case class EventStoreVersion private[eventstore] (asInt: Int) extends AnyVal {
    def next: EventStoreVersion = EventStoreVersion(asInt + 1)
  }

  object EventStoreVersion {
    val initial: EventStoreVersion = EventStoreVersion(25)
  }

  case class ProcessId private[eventstore] (asUuid: UUID) extends AnyVal

  object ProcessId {

    def apply(input: String): Either[String, ProcessId] = parseUuid(input).map(fromUuid)

    def fromUuid(input: UUID): ProcessId = ProcessId(input)

    def generate: UIO[ProcessId] = Random.nextUUID.map(fromUuid)
  }

  case class AggregateId private[eventstore] (asUuid: UUID) extends AnyVal

  object AggregateId {

    def apply(input: String): Either[String, AggregateId] = parseUuid(input).map(fromUuid)

    def fromUuid(input: UUID): AggregateId = AggregateId(input)

    def generate: UIO[AggregateId] = Random.nextUUID.map(fromUuid)
  }

  private def parseUuid(input: String): Either[String, UUID] =
    Try(UUID.fromString(input)).fold(
      _ => Left(s"$input has not UUID format"),
      uuid => Right(uuid)
    )

  case class AggregateName private[eventstore] (asString: String) extends AnyVal
}
