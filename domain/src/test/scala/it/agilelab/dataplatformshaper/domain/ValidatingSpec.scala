package it.agilelab.dataplatformshaper.domain

import io.circe.*
import io.circe.parser.*
import io.circe.yaml.syntax.*
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.*
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.collection.mutable
import scala.language.{dynamics, implicitConversions}
import scala.sys.process.*

class ValidatingSpec extends AnyFlatSpec with Matchers:

  behavior of "DataType"

  "Unfolding a tuple conform to a schema" should "work" in {

    val schema: Schema = StructType(
      List(
        "organization" -> StringType(),
        "sub-organization" -> StringType(),
        "domain" -> StringType(),
        "sub-domain" -> StringType(),
        "foundation" -> DateType(),
        "foundationRepeated" -> DateType(Repeated),
        "foundationNullable" -> DateType(Nullable),
        "aTimestamp" -> TimestampDataType(),
        "aTimestampRepeated" -> TimestampDataType(Repeated),
        "aTimestampNullable" -> TimestampDataType(Nullable),
        "aDouble" -> DoubleType(),
        "aDoubleRepeated" -> DoubleType(Repeated),
        "aDoubleNullable" -> DoubleType(Nullable),
        "aJson" -> JsonType(Required),
        "aJsonRepeated" -> JsonType(Repeated),
        "aJsonNullable" -> JsonType(Nullable),
        "aFloat" -> FloatType(),
        "aFloatRepeated" -> FloatType(Repeated),
        "aFloatNullable" -> FloatType(Nullable),
        "aLong" -> LongType(),
        "aLongRepeated" -> LongType(Repeated),
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
              List(
                "nest3" -> StringType(),
                "nest4" -> StringType()
              ),
              Repeated
            )
          )
        ),
        "columns" -> StructType(
          List(
            "name" -> StringType(),
            "type" -> StringType()
          ),
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
        ZonedDateTime
          .of(2022, 10, 11, 12, 1, 12, 13, ZoneId.of("Europe/London")),
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
        parse(
          "{\"name\": \"Robert Brown\", \"age\": 45, \"city\": \"Chicago\"}"
        ).getOrElse("")
      ),
      "aJsonNullable" -> Some(
        parse(
          "{\"name\": \"Olivia Davis\", \"age\": 31, \"city\": \"Houston\"}"
        ).getOrElse("")
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

    val yamlFile = File.createTempFile("test", ".yaml")
    val cueFile = File.createTempFile("test", ".cue")
    yamlFile.deleteOnExit()
    cueFile.deleteOnExit()
    val pw1 = new PrintWriter(yamlFile)
    pw1.write(
      tupleToJson(tuple, schema)
        .map(_.asYaml.spaces2)
        .toOption
        .get
        .replace("null", "nil")
    )
    pw1.close

    val pw2 = new PrintWriter(cueFile)
    pw2.write(generateCueModel(schema))
    pw2.close

    val errors: mutable.Builder[String, List[String]] = List.newBuilder[String]
    s"cue eval ${yamlFile.getAbsolutePath} ${cueFile.getAbsolutePath} -c" ! ProcessLogger(
      _ => (),
      errors += _
    )
    errors.result().size shouldBe 0
  }

end ValidatingSpec
