package eventstore.pg

import doobie._
import eventstore._
import eventstore.pg.test.PostgresTestUtils
import eventstore.pg.test.PostgresTestUtils.DbAdmin
import zio.Scope
import zio.ULayer
import zio.ZLayer
import zio.durationInt
import zio.test._

import EventRepositorySpec._

object PostgresRawCodecs {

  def parseEvent1(input: String): Either[String, Event1] = input match {
    case """{"type": "A"}""" => Right(A)
    case s"""{"bar": $bar, "foo": $foo, "type": "B"}""" =>
      for {
        fooB <- foo.toBooleanOption.toRight(s"$foo is not a boolean")
        barI <- bar.toIntOption.toRight(s"$bar is not an int")
      } yield B(fooB, barI)
    case _ => Left("not matching")
  }

  val d1: Get[Event1] = Get[String].temap(parseEvent1)

  val e1: Put[Event1] = Put[String].contramap {
    case A    => """{"type": "A"}"""
    case b: B => s"""{"type": "B", "foo": ${b.foo}, "bar": ${b.bar}}"""
  }

  def parseEvent2(input: String): Either[String, Event2] = input match {
    case """{"type": "C"}""" => Right(C)
    case s"""{"bar": $bar, "foo": $foo, "type": "D"}""" =>
      for {
        fooB <- foo.toBooleanOption.toRight(s"$foo is not a boolean")
        barI <- bar.toIntOption.toRight(s"$bar is not an int")
      } yield D(fooB, barI)
    case _ => Left("not matching")
  }

  val d2: Get[Event2] = Get[String].temap(parseEvent2)

  val e2: Put[Event2] = Put[String].contramap {
    case C    => """{"type": "C"}"""
    case d: D => s"""{"type": "D", "foo": ${d.foo}, "bar": ${d.bar}}"""
  }

  val dEvent: Get[Event] = Get[String].temap { input =>
    parseEvent1(input).orElse(parseEvent2(input))
  }

  val userJsonGet: Get[User] = Get[String].temap {
    case s"""{"type": "User", "id": "$value"}""" => Right(User(value))
    case s"""{"id": "$value", "type": "User"}""" => Right(User(value))
    case _                                       => Left("can't deserialize user")
  }
  val userJsonPut: Put[User] =
    Put[String].contramap(user => s"""{"type": "User", "id": "${user.id}"}""")

  implicit val codecs: Codecs[Get, Put] = Codecs(
    eventDecoder = dEvent,
    event1Decoder = d1,
    event1Encoder = e1,
    event2Decoder = d2,
    event2Encoder = e2,
    userDecoder = userJsonGet,
    userEncoder = userJsonPut
  )

}

object PostgresRawEventRepositorySpec extends ZIOSpec[DbAdmin] {

  override val bootstrap: ULayer[DbAdmin with TestEnvironment] = testEnvironment ++ PostgresTestUtils.adminConnection

  import PostgresRawCodecs._

  override val spec: Spec[TestEnvironment with DbAdmin with Scope, Any] = suite("postgres raw")(
    EventRepositorySpec.spec(
      ZLayer.makeSome[DbAdmin, EventRepository[Get, Put]](
        PostgresTestUtils.transactor,
        PostgresEventRepositoryLive.layer
      )
    )
  ) @@ TestAspect.timeout(2.minute) @@ TestAspect.parallelN(2)
}
