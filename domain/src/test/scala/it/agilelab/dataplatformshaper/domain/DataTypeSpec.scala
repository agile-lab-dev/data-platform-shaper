package it.agilelab.dataplatformshaper.domain

import io.circe.*
import io.circe.parser.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.model.schema.{
  DynamicTuple,
  *,
  given
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.language.{dynamics, implicitConversions}

class DataTypeSpec extends AnyFlatSpec with Matchers:

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
        "timestamp" -> TimestampType(),
        "timestampRepeated" -> TimestampType(Repeated),
        "timestampNullable" -> TimestampType(Nullable),
        "double" -> DoubleType(),
        "doubleRepeated" -> DoubleType(Repeated),
        "doubleNullable" -> DoubleType(Nullable),
        "json" -> JsonType(Required),
        "jsonRepeated" -> JsonType(Repeated),
        "jsonNullable" -> JsonType(Nullable),
        "float" -> FloatType(),
        "floatRepeated" -> FloatType(Repeated),
        "floatNullable" -> FloatType(Nullable),
        "long" -> LongType(),
        "longRepeated" -> LongType(Repeated),
        "longNullable" -> LongType(Nullable),
        "bool" -> BooleanType(),
        "boolRepeated" -> BooleanType(Repeated),
        "boolNullable" -> BooleanType(Nullable),
        "labels" -> StringType(Repeated),
        "nullable" -> IntType(Nullable),
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
      "timestamp" -> ZonedDateTime.of(
        2023,
        10,
        11,
        12,
        0,
        0,
        0,
        ZoneId.of("Europe/London")
      ),
      "timestampRepeated" -> List(
        ZonedDateTime
          .of(2022, 10, 11, 12, 1, 12, 13, ZoneId.of("Europe/London")),
        ZonedDateTime.of(2023, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
      ),
      "timestampNullable" -> None,
      "double" -> 1.23,
      "doubleRepeated" -> List(1.23, 3.21),
      "doubleNullable" -> Some(3.21),
      "json" -> parse(
        "{\"name\": \"Michael Johnson\", \"age\": 33, \"city\": \"Los Angeles\"}"
      ).getOrElse(""),
      "jsonRepeated" -> List(
        parse(
          "{\"name\": \"Alice Williams\", \"age\": 29, \"city\": \"San Francisco\"}"
        ).getOrElse(""),
        parse(
          "{\"name\": \"Robert Brown\", \"age\": 45, \"city\": \"Chicago\"}"
        ).getOrElse("")
      ),
      "jsonNullable" -> Some(
        parse(
          "{\"name\": \"Olivia Davis\", \"age\": 31, \"city\": \"Houston\"}"
        ).getOrElse("")
      ),
      "float" -> 1.23f,
      "floatRepeated" -> List(1.23f, 3.21f),
      "floatNullable" -> Some(1.23f),
      "long" -> 1L,
      "longRepeated" -> List(1L, 3L),
      "longNullable" -> Some(3L),
      "bool" -> true,
      "boolRepeated" -> List(true, false),
      "boolNullable" -> Some(false),
      "labels" -> List("label1", "label2"),
      "nullable" -> Some(1),
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

    val _ = (for {
      t1 <- tuple.replace(
        "nested/furtherNested/0/nest3",
        "pippo3",
        Some(schema)
      )
      t2 <- t1.replace("sub-organization", "Italy", Some(schema))
      t3 <- t2.replace("labels/0", "etichetta1", Some(schema))
    } yield t3) shouldBe Right(
      (
        "organization" -> "HR",
        "sub-organization" -> "Italy",
        "domain" -> "Registration",
        "sub-domain" -> "Person",
        "foundation" -> LocalDate.of(2008, 8, 26),
        "foundationRepeated" -> List(
          LocalDate.of(2008, 8, 26),
          LocalDate.of(1966, 11, 24)
        ),
        "foundationNullable" -> None,
        "timestamp" -> ZonedDateTime
          .of(2023, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London")),
        "timestampRepeated" -> List(
          ZonedDateTime
            .of(2022, 10, 11, 12, 1, 12, 13, ZoneId.of("Europe/London")),
          ZonedDateTime
            .of(2023, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
        ),
        "timestampNullable" -> None,
        "double" -> 1.23,
        "doubleRepeated" -> List(1.23, 3.21),
        "doubleNullable" -> Some(3.21),
        "json" -> parse(
          "{\"name\": \"Michael Johnson\", \"age\": 33, \"city\": \"Los Angeles\"}"
        ).getOrElse(""),
        "jsonRepeated" -> List(
          parse(
            "{\"name\": \"Alice Williams\", \"age\": 29, \"city\": \"San Francisco\"}"
          ).getOrElse(""),
          parse(
            "{\"name\": \"Robert Brown\", \"age\": 45, \"city\": \"Chicago\"}"
          ).getOrElse("")
        ),
        "jsonNullable" -> Some(
          parse(
            "{\"name\": \"Olivia Davis\", \"age\": 31, \"city\": \"Houston\"}"
          ).getOrElse("")
        ),
        "float" -> 1.23f,
        "floatRepeated" -> List(1.23f, 3.21f),
        "floatNullable" -> Some(1.23f),
        "long" -> 1L,
        "longRepeated" -> List(1L, 3L),
        "longNullable" -> Some(3L),
        "bool" -> true,
        "boolRepeated" -> List(true, false),
        "boolNullable" -> Some(false),
        "labels" -> List("etichetta1", "label2"),
        "nullable" -> Some(1),
        "nested" -> ("nest1" -> "ciccio1", "nest2" -> "ciccio2", "furtherNested" -> List(
          ("nest3" -> "pippo3", "nest4" -> "ciccio4"),
          ("nest3" -> "ciccio5", "nest4" -> "ciccio6")
        )),
        "columns" -> List(
          ("name" -> "FirstName", "type" -> "String"),
          ("name" -> "FamilyNane", "type" -> "String"),
          ("name" -> "Age", "type" -> "Int")
        )
      )
    )

    parseTuple(tuple, schema).foreach((t: Tuple) =>
      (t: DynamicTuple).nested
        .furtherNested(0)
        .nest3
        .value[String] shouldBe "ciccio3"
    )

    val res = unfoldTuple(tuple, schema, (_, _, _, _) => ())

    res should matchPattern { case Right(()) => }
  }

  "Converting back and forth a json document into a schema conforming tuple " should "work" in {

    val schema: Schema = StructType(
      List(
        "organization" -> StringType(),
        "sub-organization" -> StringType(),
        "domain" -> StringType(),
        "sub-domain" -> StringType(),
        "foundation" -> DateType(),
        "foundationRepeated" -> DateType(Repeated),
        "foundationNullable" -> DateType(Nullable),
        "timestamp" -> TimestampType(),
        "timestampRepeated" -> TimestampType(Repeated),
        "timestampNullale" -> TimestampType(Nullable),
        "double" -> DoubleType(),
        "doubleRepeated" -> DoubleType(Repeated),
        "doubleNullable" -> DoubleType(Nullable),
        "json" -> JsonType(Required),
        "jsonRepeated" -> JsonType(Repeated),
        "jsonNullable" -> JsonType(Nullable),
        "float" -> FloatType(),
        "floatRepeated" -> FloatType(Repeated),
        "floatNullable" -> FloatType(Nullable),
        "long" -> LongType(),
        "longRepeated" -> LongType(Repeated),
        "longNullable" -> LongType(Nullable),
        "bool" -> BooleanType(),
        "boolRepeated" -> BooleanType(Repeated),
        "boolNullable" -> BooleanType(Nullable),
        "labels" -> StringType(Repeated),
        "version" -> IntType(),
        "aStruct" -> StructType(
          List("nest1" -> StringType(), "nest2" -> StringType())
        ),
        "columns" -> StructType(
          List("name" -> StringType(), "type" -> StringType()),
          Repeated
        ),
        "aNullable" -> StringType(Nullable)
      )
    )

    val rawJson: String = """
    {
      "organization": "HR",
      "sub-organization": "Italy",
      "domain": "Registration",
      "sub-domain": "Person",
      "foundation": "2008-08-26",
      "foundationRepeated": ["2008-08-26", "1966-11-24"],
      "foundationNullable": null,
      "timestamp": "2023-10-11T12:00Z[Europe/London]",
      "timestampRepeated": ["2022-10-11T12:01:12.000000013+01:00[Europe/London]", "2023-10-11T12:00Z[Europe/London]"],
      "timestampNullable": null,
      "double": 1.23,
      "doubleRepeated": [1.23, 3.21],
      "doubleNullable": 3.21,
      "json": {"name": "Michael Johnson", "age": 33, "city": "Los Angeles"},
      "jsonRepeated" : [{"name": "Alice Williams", "age": 29, "city": "San Francisco"},
      {"name": "Robert Brown", "age": 45, "city": "Chicago"}],
      "jsonNullable": {"name": "Olivia Davis", "age": 31, "city": "Houston"},
      "float": 1.23,
      "floatRepeated": [1.23, 3.21],
      "floatNullable": 1.23,
      "long": 1,
      "longRepeated": [1, 3],
      "longNullable": 3,
      "bool": true,
      "boolRepeated": [true, false],
      "boolNullable": false,
      "labels": ["label1", "label2", "label3"],
      "version": 1,
      "aStruct": {
        "nest1": "ciccio1",
        "nest2": "ciccio2"
      },
      "columns": [
          {"name": "FirstName",  "type": "String"},
          {"name": "FamilyNane", "type": "String"},
          {"name": "Age",        "type": "Int"}
        ],
      "aNullable": "notNull"
    }
    """

    val parseResult1 = parse(rawJson)

    val result1 = parseResult1
      .flatMap(json => jsonToTuple(json, schema))
      .flatMap(tuple => tupleToJson(tuple, schema))
      .flatMap(json => jsonToTuple(json, schema))
      .flatMap(tuple => parseTuple(tuple, schema))

    val _ = result1 should matchPattern({ case Right(_) =>
    })

    val wrongRawJson: String =
      """
        {
          "organization": "HR",
          "sub-organization": "Italy",
          "domain": "Registration",
          "sub-domain": "Person",
          "foundation": "2008-08-26",
          "foundationRepeated": ["2008-08-26", "1966-11-24"],
          "foundationNullable": null,
          "timestamp": "2023-10-11T12:00Z[Europe/London]",
          "timestampRepeated": ["2022-10-11T12:01:12.000000013+01:00[Europe/London]", "2023-10-11T12:00Z[Europe/London]"],
          "timestampNullable": null,
          "double": 1.23,
          "doubleRepeated": [1.23, 3.21],
          "doubleNullable": 3.21,
          "json": {"name": "Michael Johnson", "age": 33, "city": "Los Angeles"},
          "jsonRepeated": [{"name": "Alice Williams", "age": 29, "city": "San Francisco"},
          "jsonNullable": {"name": "Olivia Davis", "age": 31, "city": "Houston"},
          {"name": "Robert Brown", "age": 45, "city": "Chicago"}],
          "float": 1.23,
          "floatRepeated": [1.23, 3.21],
          "floatNullable": 1.23,
          "long": 1,
          "longRepeated": [1, 3],
          "longNullable": 3,
          "bool": true,
          "boolRepeated": [true, false],
          "boolNullable": false,
          "labels": ["label1", "label2", "label3"],
          "version": 1,
          "aStruct": {
            "nest1": "ciccio1"
          },
          "aNullable": "notNull"
        }
      """

    val parseResult2 = parse(wrongRawJson)

    val result2 = parseResult2.flatMap(json => jsonToTuple(json, schema))

    result2 should matchPattern({ case Left(_) =>
    })
  }

  "Parsing a repeated struct" should "work" in {
    val schema: Schema = StructType(
      List(
        "columns" -> StructType(
          List("name" -> StringType(), "type" -> StringType()),
          Repeated
        )
      )
    )

    val tuple: Tuple = Tuple1(
      "columns" -> List(
        ("name" -> "FirstName", "type" -> "String"),
        ("name" -> "FamilyNane", "type" -> "String"),
        ("name" -> "Age", "type" -> "Int")
      )
    )

    val res = unfoldTuple(tuple, schema, (_, _, _, _) => ())
    val _ = res should matchPattern { case Right(()) => }

    val json = tupleToJsonChecked(tuple, schema)

    val newTuple = jsonToTupleChecked(json, schema)

    tuple shouldBe (newTuple)
  }

end DataTypeSpec
