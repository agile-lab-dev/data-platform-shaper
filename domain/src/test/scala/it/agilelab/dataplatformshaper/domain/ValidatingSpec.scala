package it.agilelab.dataplatformshaper.domain

import cats.data.EitherT
import cats.effect.{IO, Ref}
import io.circe.*
import io.circe.parser.*
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.dataplatformshaper.domain.model.l0
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.model.schema.DataType.JsonType
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.InstanceValidationError
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import org.scalactic.Equality

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.language.{dynamics, implicitConversions}

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.asInstanceOf",
    "scalafix:DisableSyntax.isInstanceOf",
    "scalafix:DisableSyntax.=="
  )
)
class ValidatingSpec extends CommonSpec:

  given Equality[DataType] with
    def areEqual(x: DataType, y: Any): Boolean =
      x match
        case struct: StructType if y.isInstanceOf[StructType] =>
          struct === y.asInstanceOf[StructType]
        case _ =>
          x == y
    end areEqual
  end given

  given Equality[StructType] with
    def areEqual(x: StructType, y: Any): Boolean =
      val c1: Map[String, DataType] = x.records.toMap
      val c2: Map[String, DataType] = y.asInstanceOf[StructType].records.toMap
      val ret = c1.foldLeft(true)((b, p) => b && c2(p(0)) === p(1))
      ret
    end areEqual
  end given

  given cache: Ref[IO, Map[String, EntityType]] =
    Ref[IO].of(Map.empty[String, EntityType]).unsafeRunSync()

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

  val schemaAfterUpdate: Schema = StructType(
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
      "aLong" -> LongType(constraints = Some("(> 0 | < 5) & < 5")),
      "aLongRepeated" -> LongType(Repeated, constraints = Some(">=5")),
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

  val wrongSchemaAfterUpdate: Schema = StructType(
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
      "aFloatRepeated" -> DoubleType(Repeated),
      "aFloatNullable" -> FloatType(Nullable),
      "aLong" -> LongType(constraints = Some("(> 0 | < 5) & < 5")),
      "aLongRepeated" -> LongType(Repeated, constraints = Some(">=5")),
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

  val conformingTupleForUpdate: Tuple = (
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

  "Creating an EntityType instance" - {
    "works" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Rdf4jKnowledgeGraph[IO] =
          Rdf4jKnowledgeGraph[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val service = TypeManagementServiceInterpreter[IO](trservice)
        val entityType = l0.EntityType(
          "ValidationDataCollectionType",
          schema
        )
        service.create(entityType)
      } asserting (ret =>
        ret should matchPattern { case Right(_) =>
        }
      )
    }
  }

  "Creating a conforming instance for an EntityType" - {
    "works" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        iservice.create(
          "ValidationDataCollectionType",
          tuple
        )
      } asserting (_ should matchPattern { case Right(_) => })
    }
  }

  "Creating a non-conforming instance for an EntityType" - {
    "fails" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        iservice.create(
          "ValidationDataCollectionType",
          nonConformingTuple
        )
      } asserting (_ should matchPattern {
        case Left(InstanceValidationError(errors)) if errors.size.equals(4) =>
      })
    }
  }

  "Updating an Entity with conforming values" - {
    "works" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        (for {
          uid <- EitherT[IO, ManagementServiceError, String](
            iservice.create(
              "ValidationDataCollectionType",
              tuple
            )
          )
          _ <- EitherT[IO, ManagementServiceError, String](
            iservice.update(
              uid,
              conformingTupleForUpdate
            )
          )
          read <- EitherT[IO, ManagementServiceError, Entity](
            iservice.read(uid)
          )
        } yield read).value
      } asserting (entity => {
        entity should matchPattern {
          case Right(Entity(_, "ValidationDataCollectionType", _)) =>
        }
        entity match {
          case Right(Entity(_, _, data)) =>
            val x =
              tupleToJsonChecked(data, schema)
            val y = tupleToJsonChecked(
              conformingTupleForUpdate,
              schema
            )
            x shouldBe y
          case _ => fail("Unexpected pattern encountered")
        }
      })
    }
  }

  "Updating an Entity with non-conforming values" - {
    "fails" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        (for {
          uid <- EitherT[IO, ManagementServiceError, String](
            iservice.create(
              "ValidationDataCollectionType",
              tuple
            )
          )
          update <- EitherT[IO, ManagementServiceError, String](
            iservice.update(
              uid,
              nonConformingTuple
            )
          )
        } yield update).value
      } asserting {
        case Left(InstanceValidationError(errors)) =>
          withClue(
            "Update should fail with ValidationError containing specific errors: "
          ) {
            errors.size shouldBe 4
          }
        case _ =>
          fail("Expected ValidationError but received a different result")
      }
    }
  }

  "Updating an EntityType instance" - {
    "works" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val entityType = l0.EntityType(
        "UpdateDataCollectionType",
        schema
      )
      val updatedEntityType = l0.EntityType(
        "UpdateDataCollectionType",
        schemaAfterUpdate
      )
      session.use { session =>
        val repository: Rdf4jKnowledgeGraph[IO] =
          Rdf4jKnowledgeGraph[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val service = TypeManagementServiceInterpreter[IO](trservice)

        val result = for {
          _ <- service.create(entityType)
          _ <- service.updateConstraints(updatedEntityType)
          readResult <- service.read(updatedEntityType.name)
        } yield readResult

        result
      } asserting { ret =>
        ret shouldBe Right(updatedEntityType)
      }
    }
  }

  "Updating an EntityType instance with a different schema" - {
    "fails" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val entityType = l0.EntityType(
        "UpdateDataCollectionType",
        schema
      )
      val updatedEntityType = l0.EntityType(
        "UpdateDataCollectionType",
        wrongSchemaAfterUpdate
      )
      session.use { session =>
        val repository: Rdf4jKnowledgeGraph[IO] =
          Rdf4jKnowledgeGraph[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val service = TypeManagementServiceInterpreter[IO](trservice)

        val result = for {
          _ <- service.create(entityType)
          updateResult <- service.updateConstraints(updatedEntityType)
        } yield updateResult

        result
      } asserting { ret =>
        ret should matchPattern {
          case Left(ManagementServiceError.MismatchingSchemas(_)) =>
        }
      }
    }
  }

  "Creating an EntityType with wrong constraints" - {
    "fails" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val entityType = l0.EntityType(
        "TypeWithWrongConstraints",
        StructType(
          List(
            "anInt" -> IntType(constraints = Some("< NOTANUMBER"))
          )
        )
      )
      session.use { session =>
        val repository: Rdf4jKnowledgeGraph[IO] =
          Rdf4jKnowledgeGraph[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val service = TypeManagementServiceInterpreter[IO](trservice)

        val result = for {
          res <- service.create(entityType)
        } yield res
        result
      } asserting { ret =>
        ret should matchPattern {
          case Left(ManagementServiceError.InvalidConstraints(_)) =>
        }
      }
    }
  }

  "Updating an EntityType with wrong constraints" - {
    "fails" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val entityType = l0.EntityType(
        "YetAnotherType",
        StructType(
          List(
            "anInt" -> IntType(constraints = Some("< 10"))
          )
        )
      )

      val wrongEntityType = l0.EntityType(
        "YetAnotherType",
        StructType(
          List(
            "anInt" -> IntType(constraints = Some("< NOTANUMBER"))
          )
        )
      )

      session.use { session =>
        val repository: Rdf4jKnowledgeGraph[IO] =
          Rdf4jKnowledgeGraph[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val service = TypeManagementServiceInterpreter[IO](trservice)

        val result = for {
          _ <- service.create(entityType)
          res <- service.updateConstraints(wrongEntityType)
        } yield res
        result
      } asserting { ret =>
        ret should matchPattern {
          case Left(ManagementServiceError.InvalidConstraints(_)) =>
        }
      }
    }
  }

end ValidatingSpec
