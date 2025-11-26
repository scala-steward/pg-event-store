package eventstore.pg

import cats.implicits.catsSyntaxTuple2Semigroupal
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
    case """{"type": "A"}"""                            => Right(A)
    case s"""{"bar": $bar, "foo": $foo, "type": "B"}""" =>
      for {
        fooB <- foo.toBooleanOption.toRight(s"$foo is not a boolean")
        barI <- bar.toIntOption.toRight(s"$bar is not an int")
      } yield B(fooB, barI)
    case _ => Left("not matching")
  }

  def serializeEvent1(event1: Event1): String = event1 match {
    case A    => """{"type": "A"}"""
    case b: B => s"""{"type": "B", "foo": ${b.foo}, "bar": ${b.bar}}"""
  }

  val d1: Get[Event1] = Get[String].temap(parseEvent1)

  val e1: Put[Event1] = Put[String].contramap { serializeEvent1 }

  def parseEvent2(input: String): Either[String, Event2] = input match {
    case """{"type": "C"}"""                            => Right(C)
    case s"""{"bar": $bar, "foo": $foo, "type": "D"}""" =>
      for {
        fooB <- foo.toBooleanOption.toRight(s"$foo is not a boolean")
        barI <- bar.toIntOption.toRight(s"$bar is not an int")
      } yield D(fooB, barI)
    case _ => Left("not matching")
  }

  def serializeEvent2(event2: Event2): String = event2 match {
    case C    => """{"type": "C"}"""
    case d: D => s"""{"type": "D", "foo": ${d.foo}, "bar": ${d.bar}}"""

  }

  def serializeEvent(event: Event): String = event match {
    case event: Event1 => serializeEvent1(event)
    case event: Event2 => serializeEvent2(event)
  }

  def parseUser(user: String): Either[String, User] = user match {
    case s"""{"type": "User", "id": "$value"}""" => Right(User(value))
    case s"""{"id": "$value", "type": "User"}""" => Right(User(value))
    case _                                       => Left("can't deserialize user")
  }

  def parseDoneBy(input: String): Either[String, DoneBy] = input match {
    case s"""{"type": "Bob"}"""   => Right(Bob)
    case s"""{"type": "Alice"}""" => Right(Alice)
    case s"""{"type": "Pete"}"""  => Right(Pete)
    case s"""{"type": "Paul"}"""  => Right(Paul)
    case _                        => Left("can't deserialize DoneBy")
  }

  def parseDoneBy1(input: String): Either[String, DoneBy1] = input match {
    case s"""{"type": "Bob"}"""   => Right(Bob)
    case s"""{"type": "Alice"}""" => Right(Alice)
    case _                        => Left("can't deserialize DoneBy1")
  }

  def parseDoneBy2(input: String): Either[String, DoneBy2] = input match {
    case s"""{"type": "Pete"}""" => Right(Pete)
    case s"""{"type": "Paul"}""" => Right(Paul)
    case _                       => Left("can't deserialize DoneBy2")
  }

  def serializeDoneBy(doneBy: DoneBy): String =
    doneBy match {
      case value: DoneBy1 => serializeDoneBy1(value)
      case value: DoneBy2 => serializeDoneBy2(value)
    }

  def serializeDoneBy1(doneBy1: DoneBy1): String =
    doneBy1 match {
      case EventRepositorySpec.Bob   => """{"type": "Bob"}"""
      case EventRepositorySpec.Alice => """{"type": "Alice"}"""
    }

  def serializeDoneBy2(doneBy2: DoneBy2): String =
    doneBy2 match {
      case EventRepositorySpec.Pete => """{"type": "Pete"}"""
      case EventRepositorySpec.Paul => """{"type": "Paul"}"""
    }

  def serializeUser(user: User): String = s"""{"type": "User", "id": "${user.id}"}"""

  def parseAutomaticProcess(user: String): Either[String, AutomaticProcess] = user match {
    case s"""{"type": "AutomaticProcess", "id": "$value"}""" => Right(AutomaticProcess(value))
    case s"""{"id": "$value", "type": "AutomaticProcess"}""" => Right(AutomaticProcess(value))
    case _                                                   => Left("can't deserialize user")
  }

  def serializeAutomaticProcess(user: AutomaticProcess): String =
    s"""{"type": "AutomaticProcess", "id": "${user.id}"}"""

  val d2: Get[Event2] = Get[String].temap(parseEvent2)

  val e2: Put[Event2] = Put[String].contramap { serializeEvent2 }

  val dEvent: Get[Event] = Get[String].temap { parseEvent }

  private def parseEvent(input: String): Either[String, Event] = {
    parseEvent1(input).orElse(parseEvent2(input))
  }

  val userJsonGet: Get[User] = Get[String].temap { parseUser }

  val userJsonPut: Put[User] = Put[String].contramap { serializeUser }

  val event1WithUserJsonPut: Put[(Event1, User)] =
    Put[String].contramap { case (event, user) =>
      s"""{"event": ${serializeEvent1(event)}, "user": ${serializeUser(user)}}"""
    }

  val event1WithDoneBy1JsonPut: Put[(Event1, DoneBy1)] =
    Put[String].contramap { case (event, doneBy1) =>
      s"""{"event": ${serializeEvent1(event)}, "doneBy1": ${serializeDoneBy1(doneBy1)}}"""
    }

  val event1WithDoneBy1JsonGet: Get[(Event1, DoneBy1)] =
    Get[String].temap {
      case s"""{"event": $event, "doneBy1": $user}""" => (parseEvent1(event), parseDoneBy1(user)).tupled
      case s"""{"user": $user, "doneBy1": $event}"""  => (parseEvent1(event), parseDoneBy1(user)).tupled
      case _                                          => Left("can't deserialize (event1, doneby1)")
    }

  val event2WithDoneBy2JsonPut: Put[(Event2, DoneBy2)] =
    Put[String].contramap { case (event, doneBy2) =>
      s"""{"event": ${serializeEvent2(event)}, "doneBy2": ${serializeDoneBy2(doneBy2)}}"""
    }

  val event2WithDoneBy2JsonGet: Get[(Event2, DoneBy2)] =
    Get[String].temap {
      case s"""{"event": $event, "doneBy2": $user}""" => (parseEvent2(event), parseDoneBy2(user)).tupled
      case s"""{"user": $user, "doneBy2": $event}"""  => (parseEvent2(event), parseDoneBy2(user)).tupled
      case _                                          => Left("can't deserialize (event2, doneby2)")
    }

  val event2WithAutomaticProcessJsonPut: Put[(Event2, AutomaticProcess)] =
    Put[String].contramap { case (event, user) =>
      s"""{"event": ${serializeEvent2(event)}, "user": ${serializeAutomaticProcess(user)}}"""
    }

  val event1WithUserJsonGet: Get[(Event1, User)] =
    Get[String].temap {
      case s"""{"event": $event, "user": $user}""" => (parseEvent1(event), parseUser(user)).tupled
      case s"""{"user": $user, "event": $event}""" => (parseEvent1(event), parseUser(user)).tupled
      case _                                       => Left("can't deserialize (event1, user)")
    }

  val eventWithUserJsonGet: Get[(Event, User)] =
    Get[String].temap {
      case s"""{"event": $event, "user": $user}""" => (parseEvent(event), parseUser(user)).tupled
      case s"""{"user": $user, "event": $event}""" => (parseEvent(event), parseUser(user)).tupled
      case _                                       => Left("can't deserialize (event, user)")
    }

  val eventWithUserJsonPut: Put[(Event, User)] =
    Put[String].contramap { case (event, user) =>
      s"""{"event": ${serializeEvent(event)}, "user": ${serializeUser(user)}}"""
    }

  val eventWithDoneByJsonGet: Get[(Event, DoneBy)] =
    Get[String].temap {
      case s"""{"event": $event, "doneBy": $user}""" => (parseEvent(event), parseDoneBy(user)).tupled
      case s"""{"user": $user, "event": $event}"""   => (parseEvent(event), parseDoneBy(user)).tupled
      case _                                         => Left("can't deserialize (event, user)")
    }

  val eventWithDoneByJsonPut: Put[(Event, DoneBy)] =
    Put[String].contramap { case (event, doneBy) =>
      s"""{"event": ${serializeEvent(event)}, "doneBy": ${serializeDoneBy(doneBy)}}"""
    }

  val event2WithAutomaticProcessJsonGet: Get[(Event2, AutomaticProcess)] =
    Get[String].temap {
      case s"""{"event": $event, "user": $user}""" => (parseEvent2(event), parseAutomaticProcess(user)).tupled
      case s"""{"user": $user, "event": $event}""" => (parseEvent2(event), parseAutomaticProcess(user)).tupled
      case _                                       => Left("can't deserialize (event2, automaticProcess)")
    }

  implicit val codecs: Codecs[Get, Put] = Codecs(
    eventDecoder = dEvent,
    event1Decoder = d1,
    event1Encoder = e1,
    event2Decoder = d2,
    event2Encoder = e2,
    userDecoder = userJsonGet,
    userEncoder = userJsonPut,
    event1WithDoneBy1Encoder = event1WithDoneBy1JsonPut,
    event1WithDoneBy1Decoder = event1WithDoneBy1JsonGet,
    event2WithDoneBy2Encoder = event2WithDoneBy2JsonPut,
    event2WithDoneBy2Decoder = event2WithDoneBy2JsonGet,
    eventWithDoneByDecoder = eventWithDoneByJsonGet,
    eventWithUserEncoder = eventWithUserJsonPut,
    eventWithUserDecoder = eventWithUserJsonGet
  )

}

object PostgresRawEventRepositorySpec extends ZIOSpec[DbAdmin] {

  override val bootstrap: ULayer[DbAdmin with TestEnvironment] = testEnvironment ++ PostgresTestUtils.adminConnection

  import PostgresRawCodecs._

  val layer: ZLayer[DbAdmin, Nothing, EventRepository[Get, Put]] = ZLayer.makeSome[DbAdmin, EventRepository[Get, Put]](
    PostgresTestUtils.transactor,
    PostgresEventRepositoryLive.layer
  )

  override val spec: Spec[TestEnvironment with DbAdmin with Scope, Any] = suite("postgres raw")(
    EventRepositorySpec.spec(layer)
  ) @@ TestAspect.timeout(2.minute) @@ TestAspect.parallelN(2)
}
