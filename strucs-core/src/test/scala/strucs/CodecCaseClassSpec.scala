package strucs

import org.scalatest.{FlatSpec, Matchers}
import strucs.CodecCaseClassSpec.Person
import strucs.EncoderSpec.{Age, City, Name}

import scala.language.experimental.macros

class CodecCaseClassSpec extends FlatSpec with Matchers {
  "Case class codec" should "???" in {
    val personStruct = Struct.empty + Name("Bart") + Age(10) + City("Springfield")
    val personCaseClass = Person(Name("Bart"), Age(10), City("Springfield"))
    val composeCodec = new ComposeCodecCaseClass[Person]
    implicit val composeCodecInstance: ComposeCodec[composeCodec.CodecCaseClassA] = composeCodec.ComposeCodecCodecCaseClassImpl
    //val codec = composeCodec.makeCodec[personStruct.Mixin]
    val codec = CodecCaseClass[CodecCaseClassSpec.Person, personStruct.type]()

    codec.encode(personCaseClass) should be(personStruct)
    codec.decode(personStruct) should be(personCaseClass)
  }
}


class ComposeCodecCaseClass[A <: Product] {
  trait CodecCaseClassA[ B] {
    def encode(a: A): B

    def decode(b: B): A
  }

  def makeCodec[T]: CodecCaseClassA[Struct[T]] = macro ComposeCodec.macroImpl[CodecCaseClassA[_], T] //ComposeCodec.makeCodec[CodecCaseClassA, T]

  implicit object ComposeCodecCodecCaseClassImpl extends ComposeCodec[CodecCaseClassA] {
    /** Build a Codec for an empty Struct */
    override def zero: CodecCaseClassA[Struct[Nil]] = ???

    /** Build a Codec using a field codec a and a codec b for the rest of the Struct */
    override def prepend[F: StructKeyProvider, B](ca: CodecCaseClassA[F], cb: ⇒ CodecCaseClassA[Struct[B]]): CodecCaseClassA[Struct[F with B]] = ???
  }

  implicit val nameInstance: CodecCaseClassA[Name] = new CodecCaseClassA[Name] {
    override def encode(a: A): Name = ???

    override def decode(b: Name): A = ???
  }

  val testInstance = makeCodec[Name with Nil]
}

trait CodecCaseClass[A <: Product, B] {
  def encode(a: A): B

  def decode(b: B): A
}

object CodecCaseClass {


  def apply[A <: Product, B <: Struct[_]]() = new CodecCaseClass[A, B] {
    override def encode(a: A): B = ???

    override def decode(b: B): A = ???
  }
}


object CodecCaseClassSpec {

  case class Person(
    name: Name,
    age: Age,
    city: City
  )

}
