package eventstore.ziojson

import eventstore.ziojson.E.EA
import eventstore.ziojson.E.EB
import eventstore.ziojson.F.FA
import eventstore.ziojson.F.FB
import eventstore.ziojson.G.GA
import eventstore.ziojson.G.GB
import izumi.reflect.Tag
import zio.json.DecoderOps
import zio.json.DeriveJsonDecoder
import zio.json.DeriveJsonEncoder
import zio.json.EncoderOps
import zio.json.JsonDecoder
import zio.json.JsonEncoder
import zio.test.*
import zio.test.Assertion.*

case class A(v1: Int)
object A:
  given JsonDecoder[A] = DeriveJsonDecoder.gen[A]
  given JsonEncoder[A] = DeriveJsonEncoder.gen[A]

case class B(x1: String)
object B:
  given JsonDecoder[B] = DeriveJsonDecoder.gen[B]
  given JsonEncoder[B] = DeriveJsonEncoder.gen[B]

case class C(z1: Long)
object C:
  given JsonDecoder[C] = DeriveJsonDecoder.gen[C]
  given JsonEncoder[C] = DeriveJsonEncoder.gen[C]

enum E:
  case EA(vA: Int)
  case EB(vB: String, vC: Int)
object E:
  given JsonDecoder[E] = DeriveJsonDecoder.gen[E]
  given JsonEncoder[E] = DeriveJsonEncoder.gen[E]

enum F:
  case FA(vA: Int, vB: String)
  case FB(vC: Int)
object F:
  given JsonDecoder[F] = DeriveJsonDecoder.gen[F]
  given JsonEncoder[F] = DeriveJsonEncoder.gen[F]


enum G:
  case GA(vA: Int)
  case GB(vB: String, vC: Int)
object G:
  given JsonDecoder[G] = DeriveJsonDecoder.gen[G]
  given JsonEncoder[G] = DeriveJsonEncoder.gen[G]

enum H:
  case HA(vA: Int, vB: String)
  case HB(vC: Int)
object H:
  given JsonDecoder[H] = DeriveJsonDecoder.gen[H]
  given JsonEncoder[H] = DeriveJsonEncoder.gen[H]


object TupleCodecSpec extends ZIOSpecDefault:

  val spec = suite("TupleCodec")(
    suite("simple case classes")(
      test("should encode a tuple2") {
        given JsonEncoder[(A, B)] = JsonEncoder.tuple2[A, B]
        assert((A(12), B("x")).toJson)(equalTo("""[{"v1":12},{"x1":"x"}]"""))
      },
      test("should decode a tuple2") {
        given JsonDecoder[(A, B)] = JsonDecoder.tuple2[A, B]
        assert("""[{"v1":12},{"x1":"x"}]""".fromJson[(A, B)])(isRight(equalTo((A(12), B("x")))))
      },
      test("should roundtrip a tuple2") {
        given JsonDecoder[(A, B)] = JsonDecoder.tuple2[A, B]
        given JsonEncoder[(A, B)] = JsonEncoder.tuple2[A, B]
        val input = (A(12), B("x"))
        assert(input.toJson.fromJson[(A, B)])(isRight(equalTo(input)))
      }
    ),
    suite("enum")(
      test("should encode a tuple2") {
        given JsonEncoder[(E, F)] = JsonEncoder.tuple2[E, F]
        assert((EA(12), FB(13)).toJson)(equalTo("""[{"EA":{"vA":12}},{"FB":{"vC":13}}]"""))
      },
      test("should decode a tuple2") {
        given JsonDecoder[(E, F)] = JsonDecoder.tuple2[E, F]
        assert("""[{"EA":{"vA":12}},{"FB":{"vC":13}}]""".fromJson[(E, F)])(isRight(equalTo((EA(12), FB(13)))))
      },
      test("should roundtrip a tuple2") {
        given JsonDecoder[(E, F)] = JsonDecoder.tuple2[E, F]
        given JsonEncoder[(E, F)] = JsonEncoder.tuple2[E, F]
        val input = (EB("a", 12), FA(33, "x"))
        assert(input.toJson.fromJson[(E, F)])(isRight(equalTo(input)))
      }
    )
  )

object UnionCodecSpec extends ZIOSpecDefault:
  val spec = suite("UnionCodec")(
    suite("simple case classes")(
      test("should encode A with type A|B") {
        given JsonEncoder[A | B] = UnionCodec.simpleTypeEncoder[A, B]
        val input: A | B = A(12)
        assert(input.toJson)(equalTo("""{"A":{"v1":12}}"""))
      },
      test("should encode B with type A|B") {
        given JsonEncoder[A | B] = UnionCodec.simpleTypeEncoder[A, B]
        val input: A | B = B("blabla")
        assert(input.toJson)(equalTo("""{"B":{"x1":"blabla"}}"""))
      },
      test("should encode A with type A|B|C") {
        given JsonEncoder[A | B | C] = {
          given JsonEncoder[B | C] = UnionCodec.simpleTypeEncoder[B, C]
          UnionCodec.simpleTypeEncoder[A, B | C]
        }
        val input: A | B | C = A(12)
        assert(input.toJson)(equalTo("""{"A":{"v1":12}}"""))
      },
      test("should encode B with type A|B|C") {
        given JsonEncoder[A | B | C] = {
          given JsonEncoder[B | C] = UnionCodec.simpleTypeEncoder[B, C]
          UnionCodec.simpleTypeEncoder[A, B | C]
        }
        val input: A | B | C = B("blabla")
        assert(input.toJson)(equalTo("""{"B":{"x1":"blabla"}}"""))
      },
      test("should encode C with type A|B|C") {
        given JsonEncoder[A | B | C] = {
          given JsonEncoder[B | C] = UnionCodec.simpleTypeEncoder[B, C]
          UnionCodec.simpleTypeEncoder[A, B | C]
        }
        val input: A | B | C = C(12L)
        assert(input.toJson)(equalTo("""{"C":{"z1":12}}"""))
      },
      test("should decode A with type A|B") {
        given JsonDecoder[A | B] = UnionCodec.simpleTypeDecoder[A, B]

        assert("""{"A":{"v1":12}}""".fromJson[A | B])(equalTo(A(v1 = 12)))
      } @@ TestAspect.ignore // see FIXME in simpleTypeDecoder
    ),
    suite("enum")(
      test("should encode EA with type E|F") {
        given JsonEncoder[E | F] = UnionCodec.sumTypeEncoder[E, F]

        val value: E | F = EA(vA = 12)
        assert(value.toJson)(equalTo("""{"EA":{"vA":12}}"""))
      },
      test("should encode EB with type E|F") {
        given JsonEncoder[E | F] = UnionCodec.sumTypeEncoder[E, F]
        val input: E | F = EB(vB = "blabla", vC = 12)
        assert(input.toJson)(equalTo("""{"EB":{"vB":"blabla","vC":12}}"""))
      },
      test("should decode EA with type E|F") {
        given JsonDecoder[E | F] = UnionCodec.sumTypeDecoder[E, F]

        assert("""{"EA":{"vA":12}}""".fromJson[E | F])(isRight(equalTo(EA(vA = 12))))
      },
      test("should decode EB with type E|F") {
        given JsonDecoder[E | F] = UnionCodec.sumTypeDecoder[E, F]

        assert("""{"EB":{"vB":"blabla","vC":12}}""".fromJson[E | F])(isRight(equalTo(EB(vB = "blabla", vC = 12))))
      }
    ),
    suite("union of tuples")(
      test("should decode (E, G) with type (E, G)|(F, H)") {
        given JsonDecoder[(E, G)] = JsonDecoder.tuple2[E, G]
        given JsonDecoder[(F, H)] = JsonDecoder.tuple2[F, H]
        given JsonDecoder[(E, G)|(F, H)] = UnionCodec.sumTypeDecoder[(E, G), (F, H)]

        assert("""[{"EB":{"vB":"blabla","vC":12}},{"GA":{"vA": 3}}]""".fromJson[(E, G)|(F, H)])(isRight(equalTo((EB(vB = "blabla", vC = 12), GA(vA = 3)))))
      }
    )

  )
