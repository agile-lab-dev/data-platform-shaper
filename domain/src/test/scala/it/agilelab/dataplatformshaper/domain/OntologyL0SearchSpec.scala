package it.agilelab.dataplatformshaper.domain

import cats.effect.IO
import io.chrisdavenport.mules.caffeine.CaffeineCache
import io.chrisdavenport.mules.{Cache, TimeSpec}
import io.circe.parser.parse
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.dataplatformshaper.domain.model.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import org.scalactic.Equality
import org.scalatest.Inside.inside

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.collection.immutable.List
import scala.concurrent.duration.*
import scala.language.postfixOps
import scala.util.Right

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.asInstanceOf",
    "scalafix:DisableSyntax.isInstanceOf",
    "scalafix:DisableSyntax.=="
  )
)
class OntologyL0SearchSpec extends CommonSpec:

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

  given cache: Cache[IO, String, EntityType] = CaffeineCache
    .build[IO, String, EntityType](
      Some(TimeSpec.unsafeFromDuration(1800.second)),
      None,
      None
    )
    .unsafeRunSync()

  val fileBasedDataCollectionTypeSchema: StructType = StructType(
    List(
      "organization" -> StringType(),
      "sub-organization" -> StringType(),
      "domain" -> StringType(),
      "sub-domain" -> StringType(),
      "foundation" -> DateType(),
      "labels" -> StringType(Repeated),
      "aString" -> StringType(),
      "optionalString" -> StringType(Nullable),
      "aJson" -> JsonType(Required),
      "repeatedJson" -> JsonType(Repeated),
      "optionalJson" -> JsonType(Nullable),
      "emptyOptionalString" -> StringType(Nullable),
      "repeatedString" -> StringType(Repeated),
      "emptyRepeatedString" -> StringType(Repeated),
      "optionalDate" -> DateType(Nullable),
      "emptyOptionalDate" -> DateType(Nullable),
      "repeatedDate" -> DateType(Repeated),
      "emptyRepeatedDate" -> DateType(Repeated),
      "aTimestamp" -> TimestampType(Required),
      "repeatedTimestamp" -> TimestampType(Repeated),
      "optionalTimestamp" -> TimestampType(Nullable),
      "timestampStruct" -> StructType(
        List(
          "aTimestamp" -> TimestampType(Required),
          "repeatedTimestamp" -> TimestampType(Repeated),
          "optionalTimestamp" -> TimestampType(Nullable)
        )
      ),
      "dateStruct" -> StructType(
        List(
          "aDate" -> DateType(Required),
          "repeatedDate" -> DateType(Repeated),
          "optionalDate" -> DateType(Nullable)
        )
      ),
      "doubleStruct" -> StructType(
        List(
          "aDouble" -> DoubleType(),
          "doubleRepeated" -> DoubleType(Repeated),
          "doubleNullable" -> DoubleType(Nullable)
        )
      ),
      "floatStruct" -> StructType(
        List(
          "aFloat" -> FloatType(),
          "floatRepeated" -> FloatType(Repeated),
          "floatNullable" -> FloatType(Nullable)
        )
      ),
      "longStruct" -> StructType(
        List(
          "aLong" -> LongType(),
          "longRepeated" -> LongType(Repeated),
          "longNullable" -> LongType(Nullable),
          "furtherNestedLongStruct" -> StructType(
            List("furtherNestedLong" -> LongType())
          )
        )
      ),
      "boolStruct" -> StructType(
        List(
          "aBool" -> BooleanType(),
          "boolRepeated" -> BooleanType(Repeated),
          "boolNullable" -> BooleanType(Nullable)
        )
      ),
      "aDouble" -> DoubleType(),
      "doubleRepeated" -> DoubleType(Repeated),
      "doubleNullable" -> DoubleType(Nullable),
      "aFloat" -> FloatType(),
      "floatRepeated" -> FloatType(Repeated),
      "floatNullable" -> FloatType(Nullable),
      "aLong" -> LongType(Required),
      "longRepeated" -> LongType(Repeated),
      "longNullable" -> LongType(Nullable),
      "aBool" -> BooleanType(),
      "boolRepeated" -> BooleanType(Repeated),
      "boolNullable" -> BooleanType(Nullable),
      "anInt" -> IntType(),
      "optionalInt" -> IntType(Nullable),
      "emptyOptionalInt" -> IntType(Nullable),
      "repeatedInt" -> IntType(Repeated),
      "emptyRepeatedInt" -> IntType(Repeated),
      "struct" -> StructType(
        List(
          "nest1" -> StringType(),
          "nest2" -> StringType(),
          "nest3" -> BooleanType(),
          "intList" -> IntType(Repeated)
        )
      ),
      "optionalStruct" -> StructType(
        List("nest1" -> StringType(), "nest2" -> StringType()),
        Nullable
      ),
      "emptyOptionalStruct" -> StructType(
        List("nest1" -> StringType(), "nest2" -> StringType()),
        Nullable
      ),
      "columns" -> StructType(
        List("name" -> StringType(), "type" -> StringType()),
        Repeated
      )
    )
  )

  val fileBasedDataCollectionTuple: Tuple = (
    "organization" -> "HR",
    "sub-organization" -> "Any",
    "domain" -> "Registrations",
    "sub-domain" -> "People",
    "foundation" -> LocalDate.of(2008, 8, 26),
    "labels" -> List("label1", "label2", "label3"),
    "aString" -> "str",
    "optionalString" -> Some("str"),
    "aJson" -> parse(
      "{\n  \"MagicLamp\": {\n    \"color\": \"golden\",\n    \"age\": \"centuries old\",\n    \"origin\": \"mystical realm\",\n    \"size\": {\n      \"height\": \"15cm\",\n      \"width\": \"30cm\"\n    },\n    \"abilities\": [\n      \"granting wishes\",\n      \"glowing in the dark\",\n      \"levitation\"\n    ],\n    \"previousOwners\": [\n      \"Elminster Aumar\",\n      \"a lost pirate\",\n      \"an unknown traveler\"\n    ],\n    \"currentLocation\": \"hidden in an ancient cave\",\n    \"condition\": \"slightly worn but still functional\"\n  }\n}"
    ).getOrElse(""),
    "repeatedJson" -> List(
      parse(
        "{\n  \"name\": \"John Doe\",\n  \"age\": 30,\n  \"city\": \"New York\"\n}"
      ).getOrElse(""),
      parse(
        "{\n  \"name\": \"Eleanor Smith\",\n  \"age\": 42,\n  \"city\": \"Miami\"\n}"
      ).getOrElse(""),
      parse(
        "{\n  \"name\": \"David Johnson\",\n  \"age\": 35,\n  \"city\": \"Seattle\"\n}"
      ).getOrElse("")
    ),
    "optionalJson" -> Some(
      parse(
        "{\n  \"name\": \"Sophie Williams\",\n  \"age\": 28,\n  \"city\": \"Denver\"\n}"
      ).getOrElse("")
    ),
    "emptyOptionalString" -> None,
    "repeatedString" -> List("str1", "str2", "str3"),
    "emptyRepeatedString" -> List(),
    "optionalDate" -> Some(LocalDate.of(2008, 8, 26)),
    "emptyOptionalDate" -> None,
    "repeatedDate" -> List(
      LocalDate.of(2008, 8, 26),
      LocalDate.of(1966, 11, 24)
    ),
    "emptyRepeatedDate" -> List(),
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
    "repeatedTimestamp" -> List(
      ZonedDateTime.of(2023, 10, 12, 12, 0, 0, 0, ZoneId.of("Europe/London")),
      ZonedDateTime.of(2024, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
    ),
    "optionalTimestamp" -> Some(
      ZonedDateTime.of(2023, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
    ),
    "timestampStruct" -> (
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
      "repeatedTimestamp" -> List(
        ZonedDateTime
          .of(2023, 11, 11, 12, 1, 12, 13, ZoneId.of("Europe/London")),
        ZonedDateTime.of(2024, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
      ),
      "optionalTimestamp" -> Some(
        ZonedDateTime.of(2023, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
      )
    ),
    "dateStruct" -> (
      "aDate" -> LocalDate.of(2009, 8, 26),
      "repeatedDate" -> List(
        LocalDate.of(2009, 8, 26),
        LocalDate.of(2000, 6, 19)
      ),
      "optionalDate" -> Some(LocalDate.of(2008, 9, 26))
    ),
    "doubleStruct" -> (
      "aDouble" -> 1.23,
      "doubleRepeated" -> List(1.23, 3.21),
      "doubleNullable" -> Some(1.23)
    ),
    "floatStruct" -> (
      "aFloat" -> 1.23f,
      "floatRepeated" -> List(1.23f, 3.21f),
      "floatNullable" -> Some(1.23f)
    ),
    "longStruct" -> (
      "aLong" -> 10L,
      "longRepeated" -> List(10L, 20L),
      "longNullable" -> Some(30L),
      "furtherNestedLongStruct" -> Tuple1("furtherNestedLong" -> 100L)
    ),
    "boolStruct" -> (
      "aBool" -> true,
      "boolRepeated" -> List(true, false).sorted,
      "boolNullable" -> Some(true)
    ),
    "aDouble" -> 1.23,
    "doubleRepeated" -> List(1.23, 3.21),
    "doubleNullable" -> Some(1.23),
    "aFloat" -> 1.23f,
    "floatRepeated" -> List(1.23f, 3.21f),
    "floatNullable" -> Some(1.23f),
    "aLong" -> 10L,
    "longRepeated" -> List(10L, 20L),
    "longNullable" -> Some(30L),
    "aBool" -> true,
    "boolRepeated" -> List(true, false).sorted,
    "boolNullable" -> Some(true),
    "anInt" -> 10,
    "optionalInt" -> Some(10),
    "emptyOptionalInt" -> None,
    "repeatedInt" -> List(10, 20, 30),
    "emptyRepeatedInt" -> List(),
    "struct" -> (
      "nest1" -> "ciccio1",
      "nest2" -> "ciccio2",
      "nest3" -> true,
      "intList" -> List(1, 2, 3)
    ),
    "optionalStruct" -> Some(("nest1" -> "ciccio1", "nest2" -> "ciccio2")),
    "emptyOptionalStruct" -> None,
    "columns" -> List(
      ("type" -> "Int", "name" -> "Age"),
      ("type" -> "String", "name" -> "FamilyNane"),
      ("type" -> "String", "name" -> "FirstName")
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
        val entityType = EntityType(
          "FileBasedDataCollectionType",
          Set("DataCollection"),
          fileBasedDataCollectionTypeSchema
        )
        trservice.create(Trait("DataCollection", None)) *>
          service.create(entityType) *>
          service.read("FileBasedDataCollectionType")
      } asserting (ret =>
        inside(ret) { case Right(entity) =>
          val _ = entity.name shouldBe "FileBasedDataCollectionType"
          val _ = entity.traits shouldBe Set("DataCollection")
          entity.baseSchema === fileBasedDataCollectionTypeSchema shouldBe true
        }
      )
    }
  }

  "Creating an instance for an EntityType" - {
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
          "FileBasedDataCollectionType",
          fileBasedDataCollectionTuple
        )
      } asserting (_ should matchPattern { case Right(_) => })
    }
  }

  "Searching an instance for an EntityType" - {
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
        val entityType = "FileBasedDataCollectionType"

        val predicate1 =
          " longStruct / furtherNestedLongStruct / furtherNestedLong = 100 AND longStruct / aLong < 20 AND organization = 'HR' "

        val predicate2 =
          " organization <> 'HR' "

        val predicate3 =
          " struct / nest3 = true "

        for {
          resp1 <- iservice.list(
            entityType,
            predicate1,
            returnEntities = false,
            None
          )
          resp2 <- iservice.list(
            entityType,
            predicate2,
            returnEntities = false,
            None
          )
          resp3 <- iservice.list(
            entityType,
            predicate3,
            returnEntities = false,
            None
          )
        } yield (resp1, resp2, resp3)

      } asserting (resp =>
        inside(resp) { case (Right(list1), Right(list2), Right(list3)) =>
          val _ = assert(list1.size === 1)
          val _ = assert(list2.size === 0)
          assert(list3.size === 1)
        }
      )
    }
  }

end OntologyL0SearchSpec
