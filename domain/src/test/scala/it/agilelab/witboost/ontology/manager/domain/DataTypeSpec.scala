package it.agilelab.witboost.ontology.manager.domain

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

end DataTypeSpec
