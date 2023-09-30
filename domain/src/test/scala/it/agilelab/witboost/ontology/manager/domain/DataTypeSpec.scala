package it.agilelab.witboost.ontology.manager.domain

import io.circe.*
import io.circe.parser.*
import it.agilelab.witboost.ontology.manager.domain.model.schema.Mode.*
import it.agilelab.witboost.ontology.manager.domain.model.schema.{*, given}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
        "labels" -> StringType(Repeated),
        "nullable" -> IntType(Nullable),
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
        )
      )
    )

    val tuple: Tuple = (
      "organization" -> "HR",
      "sub-organization" -> "Any",
      "domain" -> "Registration",
      "sub-domain" -> "Person",
      "labels" -> List("label1", "label2"),
      "nullable" -> Some(1),
      "nested" -> ("nest1" -> "ciccio1", "nest2" -> "ciccio2", "furtherNested" -> List(
        ("nest3" -> "ciccio3", "nest4" -> "ciccio4"),
        ("nest3" -> "ciccio5", "nest4" -> "ciccio6")
      ))
    )

    (for {
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
        "labels" -> List("etichetta1", "label2"),
        "nullable" -> Some(1),
        "nested" -> ("nest1" -> "ciccio1", "nest2" -> "ciccio2", "furtherNested" -> List(
          ("nest3" -> "pippo3", "nest4" -> "ciccio4"),
          ("nest3" -> "ciccio5", "nest4" -> "ciccio6")
        ))
      )
    )

    parseTuple(tuple, schema).foreach((t: Tuple) =>
      (t: DynamicTuple).nested
        .furtherNested(0)
        .nest3
        .value[String] shouldBe "ciccio3"
    )

    val res = unfoldTuple(tuple, schema, (_, _, _) => ())

    res should matchPattern { case Right(()) => }
  }

  "Converting a json document into a schema conforming tuple " should "work" in {

    val schema: Schema = StructType(
      List(
        "organization" -> StringType(),
        "sub-organization" -> StringType(),
        "domain" -> StringType(),
        "sub-domain" -> StringType(),
        "labels" -> StringType(Repeated),
        "version" -> IntType(),
        "aStruct" -> StructType(
          List(
            "nest1" -> StringType(),
            "nest2" -> StringType()
          )
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
      "labels": ["label1", "label2", "label3"],
      "version": 1,
      "aStruct": {
        "nest1": "ciccio1",
        "nest2": "ciccio2"
      },
      "aNullable": "notNull"
    }
    """

    val parseResult1 = parse(rawJson)

    val result1 = parseResult1
      .flatMap(json => jsonToTuple(json, schema))
      .flatMap(tuple => parseTuple(tuple, schema))

    result1 should matchPattern({ case Right(_) =>
    })

    val wrongRawJson: String =
      """
        {
          "organization": "HR",
          "sub-organization": "Italy",
          "domain": "Registration",
          "sub-domain": "Person",
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

end DataTypeSpec
