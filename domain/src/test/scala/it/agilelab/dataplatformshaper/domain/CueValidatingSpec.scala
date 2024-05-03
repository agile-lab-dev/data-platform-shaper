package it.agilelab.dataplatformshaper.domain

import io.circe.*
import io.circe.parser.*
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.language.{dynamics, implicitConversions}

class CueValidatingSpec extends AnyFlatSpec with Matchers:

  behavior of "CueValidating"

  val schema: Schema = StructType(
    List(
      "organization" -> StringType(),
      "sub-organization" -> StringType(),
      "domain" -> StringType(),
      "sub-domain" -> StringType(),
      "foundation" -> DateType(),
      "foundationRepeated" -> DateType(Repeated),
      "foundationNullable" -> DateType(Nullable),
      "aTimestamp" -> TimestampType(),
      "aTimestampRepeated" -> TimestampType(Repeated),
      "aTimestampNullable" -> TimestampType(Nullable),
      "aDouble" -> DoubleType(),
      "aDoubleRepeated" -> DoubleType(Repeated),
      "aDoubleNullable" -> DoubleType(Nullable),
      "aJson" -> JsonType(Required),
      "aJsonRepeated" -> JsonType(Repeated),
      "aJsonNullable" -> JsonType(Nullable),
      "aFloat" -> FloatType(),
      "aFloatRepeated" -> FloatType(Repeated),
      "aFloatNullable" -> FloatType(Nullable),
      "aLong" -> LongType(constraints = Some("(> 0 | < 2) & < 2")),
      "aLongRepeated" -> LongType(Repeated, constraints = Some(">=1")),
      "aLongNullable" -> LongType(Nullable),
      "aBool" -> BooleanType(),
      "aBoolRepeated" -> BooleanType(Repeated),
      "aBoolNullable" -> BooleanType(Nullable),
      "labels" -> StringType(Repeated),
      "aIntNullable" -> IntType(Nullable),
      "nested" -> StructType(
        List(
          "nest1" -> StringType(),
          "nest2" -> StringType(),
          "furtherNested" -> StructType(
            List("nest3" -> StringType(), "nest4" -> StringType()),
            Repeated
          )
        )
      ),
      "columns" -> StructType(
        List("name" -> StringType(), "type" -> StringType()),
        Repeated
      )
    )
  )

  val tuple: Tuple = (
    "organization" -> "HR",
    "sub-organization" -> "Any",
    "domain" -> "Registration",
    "sub-domain" -> "Person",
    "foundation" -> LocalDate.of(2008, 8, 26),
    "foundationRepeated" -> List(
      LocalDate.of(2008, 8, 26),
      LocalDate.of(1966, 11, 24)
    ),
    "foundationNullable" -> None,
    "aTimestamp" -> ZonedDateTime.of(
      2023,
      10,
      11,
      12,
      0,
      0,
      0,
      ZoneId.of("Europe/London")
    ),
    "aTimestampRepeated" -> List(
      ZonedDateTime.of(2022, 10, 11, 12, 1, 12, 13, ZoneId.of("Europe/London")),
      ZonedDateTime.of(2023, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
    ),
    "aTimestampNullable" -> None,
    "aDouble" -> 1.23,
    "aDoubleRepeated" -> List(1.23, 3.21),
    "aDoubleNullable" -> Some(3.21),
    "aJson" -> parse(
      "{\"name\": \"Michael Johnson\", \"age\": 33, \"city\": \"Los Angeles\"}"
    ).getOrElse(""),
    "aJsonRepeated" -> List(
      parse(
        "{\"name\": \"Alice Williams\", \"age\": 29, \"city\": \"San Francisco\"}"
      ).getOrElse(""),
      parse("{\"name\": \"Robert Brown\", \"age\": 45, \"city\": \"Chicago\"}")
        .getOrElse("")
    ),
    "aJsonNullable" -> Some(
      parse("{\"name\": \"Olivia Davis\", \"age\": 31, \"city\": \"Houston\"}")
        .getOrElse("")
    ),
    "aFloat" -> 1.23f,
    "aFloatRepeated" -> List(1.23f, 3.21f),
    "aFloatNullable" -> Some(1.23f),
    "aLong" -> 1L,
    "aLongRepeated" -> List(1L, 3L),
    "aLongNullable" -> Some(3L),
    "aBool" -> true,
    "aBoolRepeated" -> List(true, false),
    "aBoolNullable" -> Some(false),
    "labels" -> List("label1", "label2"),
    "aIntNullable" -> Some(1),
    "nested" -> ("nest1" -> "ciccio1", "nest2" -> "ciccio2", "furtherNested" -> List(
      ("nest3" -> "ciccio3", "nest4" -> "ciccio4"),
      ("nest3" -> "ciccio5", "nest4" -> "ciccio6")
    )),
    "columns" -> List(
      ("name" -> "FirstName", "type" -> "String"),
      ("name" -> "FamilyNane", "type" -> "String"),
      ("name" -> "Age", "type" -> "Int")
    )
  )

  val nonConformingTuple: Tuple = (
    "organization" -> "HR",
    "sub-organization" -> "Any",
    "domain" -> "Registration",
    "sub-domain" -> "Person",
    "foundation" -> LocalDate.of(2008, 8, 26),
    "foundationRepeated" -> List(
      LocalDate.of(2008, 8, 26),
      LocalDate.of(1966, 11, 24)
    ),
    "foundationNullable" -> None,
    "aTimestamp" -> ZonedDateTime.of(
      2023,
      10,
      11,
      12,
      0,
      0,
      0,
      ZoneId.of("Europe/London")
    ),
    "aTimestampRepeated" -> List(
      ZonedDateTime.of(2022, 10, 11, 12, 1, 12, 13, ZoneId.of("Europe/London")),
      ZonedDateTime.of(2023, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
    ),
    "aTimestampNullable" -> None,
    "aDouble" -> 1.23,
    "aDoubleRepeated" -> List(1.23, 3.21),
    "aDoubleNullable" -> Some(3.21),
    "aJson" -> parse(
      "{\"name\": \"Michael Johnson\", \"age\": 33, \"city\": \"Los Angeles\"}"
    ).getOrElse(""),
    "aJsonRepeated" -> List(
      parse(
        "{\"name\": \"Alice Williams\", \"age\": 29, \"city\": \"San Francisco\"}"
      ).getOrElse(""),
      parse("{\"name\": \"Robert Brown\", \"age\": 45, \"city\": \"Chicago\"}")
        .getOrElse("")
    ),
    "aJsonNullable" -> Some(
      parse("{\"name\": \"Olivia Davis\", \"age\": 31, \"city\": \"Houston\"}")
        .getOrElse("")
    ),
    "aFloat" -> 1.23f,
    "aFloatRepeated" -> List(1.23f, 3.21f),
    "aFloatNullable" -> Some(1.23f),
    "aLong" -> 5L,
    "aLongRepeated" -> List(0L, 3L),
    "aLongNullable" -> Some(3L),
    "aBool" -> true,
    "aBoolRepeated" -> List(true, false),
    "aBoolNullable" -> Some(false),
    "labels" -> List("label1", "label2"),
    "aIntNullable" -> Some(1),
    "nested" -> ("nest1" -> "ciccio1", "nest2" -> "ciccio2", "furtherNested" -> List(
      ("nest3" -> "ciccio3", "nest4" -> "ciccio4"),
      ("nest3" -> "ciccio5", "nest4" -> "ciccio6")
    )),
    "columns" -> List(
      ("name" -> "FirstName", "type" -> "String"),
      ("name" -> "FamilyNane", "type" -> "String"),
      ("name" -> "Age", "type" -> "Int")
    )
  )

  "Validating a generated cue model" should "work" in {
    val res = cueValidateModel(schema)
    res match {
      case Right(_)     => succeed
      case Left(errors) => fail(s"Validation failed with errors: $errors")
    }
  }

  "Validating a generated cue model from wrong constraints" should "fail" in {
    val schemaWithWrongConstraints: Schema = StructType(
      List(
        "aString" -> StringType(constraints = None),
        "anInt" -> IntType(constraints = Some("< CIAO "))
      )
    )

    val res = cueValidateModel(schemaWithWrongConstraints)

    res match {
      case Right(_) => fail("it shouldn't be right")
      case Left(List("""anInt: reference "CIAO" not found""")) =>
        succeed
      case Left(_: List[String]) =>
        fail("")
    }
  }

  "Validating a tuple conform to a schema" should "work" in {
    val res = cueValidate(schema, tuple)
    res match {
      case Right(_)     => succeed
      case Left(errors) => fail(s"Validation failed with errors: $errors")
    }
  }

end CueValidatingSpec
