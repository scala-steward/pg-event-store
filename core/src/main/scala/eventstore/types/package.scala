package eventstore

import io.estatico.newtype.macros.newtype
import zio.Random
import zio.UIO

import java.util.UUID
import scala.util.Try

package object types {

  @newtype case class AggregateVersion private[eventstore] (asInt: Int) {
    def next: AggregateVersion = AggregateVersion(asInt + 1)
  }

  object AggregateVersion {
    val initial: AggregateVersion = AggregateVersion(0)
  }

  @newtype case class EventStoreVersion private[eventstore] (asInt: Int) {
    def next: EventStoreVersion = EventStoreVersion(asInt + 1)
  }

  object EventStoreVersion {
    val initial: EventStoreVersion = EventStoreVersion(25)
  }

  @newtype case class ProcessId private[eventstore] (val asUuid: UUID)

  object ProcessId {

    def apply(input: String): Either[String, ProcessId] = parseUuid(input).map(ProcessId(_))

    def generate: UIO[ProcessId] = Random.nextUUID.map(ProcessId(_))
  }

  @newtype case class AggregateId private[eventstore] (asUuid: UUID)

  object AggregateId {

    def apply(input: String): Either[String, AggregateId] = parseUuid(input).map(AggregateId(_))

    def generate: UIO[AggregateId] = Random.nextUUID.map(AggregateId(_))
  }

  private def parseUuid(input: String): Either[String, UUID] =
    Try(UUID.fromString(input)).fold(
      _ => Left(s"$input has not UUID format"),
      uuid => Right(uuid)
    )

  @newtype case class AggregateName private[eventstore] (asString: String)
}
