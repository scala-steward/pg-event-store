package eventstore

import eventstore.types.AggregateId
import eventstore.types.AggregateName
import eventstore.types.AggregateVersion
import eventstore.types.EventStreamId
import zio.test.Gen

object Generators {

  val versionGen: Gen[Any, AggregateVersion] =
    Gen
      .int(0, 20)
      .map(version => 0.to(version).foldLeft(AggregateVersion.initial)((v, _) => v.next))

  val aggregateIdGen: Gen[Any, AggregateId] =
    Gen.uuid.map(id => AggregateId(id))

  val aggregateNameGen: Gen[Any, AggregateName] =
    Gen.alphaNumericString.map(name => AggregateName(name))

  def streamIdGen(
      nameGen: Gen[Any, AggregateName] = aggregateNameGen
  ): Gen[Any, EventStreamId] =
    aggregateIdGen.zip(nameGen).map { case (id, name) =>
      EventStreamId(id, name)
    }

}
