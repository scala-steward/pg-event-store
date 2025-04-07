package eventstore.ziojson

import zio.Tag
import zio.json.JsonDecoder
import zio.json.JsonEncoder
import zio.json.internal.Write

object UnionCodec:
  def simpleTypeDecoder[A, B](using aDecoder: JsonDecoder[A], bDecoder: JsonDecoder[B]) =
    // FIXME: it should first read the type based on the field of the top-level object
    aDecoder.widen[A | B].orElse(bDecoder.widen[A | B])

  def simpleTypeEncoder[A, B](using aEncoder: JsonEncoder[A], bEncoder: JsonEncoder[B], aTag: Tag[A], bTag: Tag[B]) =
    new JsonEncoder[A | B] {
      override def unsafeEncode(a: A | B, indent: Option[Int], out: Write): Unit = {
        if (aTag.closestClass.isInstance(a))
        then
          if aTag.tag.decomposeUnion.sizeIs == 1
          then
            out.write(s"""{"${aTag.closestClass.getSimpleName}":""")
            aEncoder.unsafeEncode(a.asInstanceOf[A], indent, out)
            out.write("}")
          else aEncoder.unsafeEncode(a.asInstanceOf[A], indent, out)
        else if bTag.tag.decomposeUnion.sizeIs == 1
          then
            out.write(s"""{"${bTag.closestClass.getSimpleName}":""")
            bEncoder.unsafeEncode(a.asInstanceOf[B], indent, out)
            out.write("}")
          else bEncoder.unsafeEncode(a.asInstanceOf[B], indent, out)
      }
    }

  def sumTypeEncoder[A, B](using aEncoder: JsonEncoder[A], bEncoder: JsonEncoder[B], aTag: Tag[A]) =
    new JsonEncoder[A | B] {
      override def unsafeEncode(a: A | B, indent: Option[Int], out: Write): Unit = {
        if aTag.closestClass.isInstance(a)
        then aEncoder.unsafeEncode(a.asInstanceOf[A], indent, out)
        else bEncoder.unsafeEncode(a.asInstanceOf[B], indent, out)
      }
    }
  def sumTypeDecoder[A, B](using aDecoder: JsonDecoder[A], bDecoder: JsonDecoder[B]) =
    aDecoder.widen[A | B].orElse(bDecoder.widen[A | B])
