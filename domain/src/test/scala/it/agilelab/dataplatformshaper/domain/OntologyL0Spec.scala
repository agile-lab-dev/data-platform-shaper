package it.agilelab.dataplatformshaper.domain

import cats.data.*
import cats.effect.*
import io.chrisdavenport.mules.caffeine.CaffeineCache
import io.chrisdavenport.mules.{Cache, TimeSpec}
import io.circe.*
import io.circe.parser.*
import it.agilelab.dataplatformshaper.domain.common.db.Repository
import it.agilelab.dataplatformshaper.domain.model.*
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import org.scalatest.Inside.inside

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.collection.immutable.List
import scala.concurrent.duration.*
import scala.language.postfixOps
import scala.util.Right

@SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
class OntologyL0Spec extends CommonSpec:

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
      "aJson" -> JsonType(Required),
      "repeatedJson" -> JsonType(Repeated),
      "optionalJson" -> JsonType(Nullable),
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
          "longNullable" -> LongType(Nullable)
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
      "columns" -> StructType(
        List("name" -> StringType(), "type" -> StringType()),
        Repeated
      ),
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
      "longNullable" -> Some(30L)
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
    "columns" -> List(
      ("name" -> "FirstName", "type" -> "String"),
      ("name" -> "FamilyNane", "type" -> "String"),
      ("name" -> "Age", "type" -> "Int")
    ),
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
      "intList" -> List(1, 2, 3)
    ),
    "optionalStruct" -> Some(("nest1" -> "ciccio1", "nest2" -> "ciccio2")),
    "emptyOptionalStruct" -> None
  )

  val fileBasedDataCollectionTupleForUpdate: Tuple = (
    "organization" -> "HR",
    "sub-organization" -> "Any",
    "domain" -> "Registrations",
    "sub-domain" -> "People",
    "foundation" -> LocalDate.of(1969, 5, 25),
    "labels" -> List("label1", "label2", "label3"),
    "aString" -> "str",
    "optionalString" -> Some("str"),
    "aJson" -> parse(
      "{\n  \"MagicLamp\": {\n    \"color\": \"golden\",\n    \"age\": \"centuries old\",\n    \"origin\": \"mystical realm\",\n    \"size\": {\n      \"height\": \"15cm\",\n      \"width\": \"30cm\"\n    },\n    \"abilities\": [\n      \"granting wishes\",\n      \"glowing in the dark\",\n      \"levitation\"\n    ],\n    \"previousOwners\": [\n      \"Elminster Aumar\",\n      \"a famous wizard\",\n      \"an unknown traveler\"\n    ],\n    \"currentLocation\": \"hidden in an ancient cave\",\n    \"condition\": \"slightly worn but still functional\"\n  }\n}"
    ).getOrElse(""),
    "repeatedJson" -> List(
      parse(
        "{\n  \"name\": \"John Doe\",\n  \"age\": 33,\n  \"city\": \"New York\"\n}"
      ).getOrElse(""),
      parse(
        "{\n  \"name\": \"Eleanor Smith\",\n  \"age\": 45,\n  \"city\": \"Miami\"\n}"
      ).getOrElse(""),
      parse(
        "{\n  \"name\": \"David Johnson\",\n  \"age\": 18,\n  \"city\": \"Seattle\"\n}"
      ).getOrElse("")
    ),
    "optionalJson" -> Some(
      parse(
        "{\n  \"name\": \"Sophie Williams\",\n  \"age\": 28,\n  \"city\": \"Austin\"\n}"
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
      ZonedDateTime.of(2025, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London")),
      ZonedDateTime.of(2024, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
    ),
    "optionalTimestamp" -> Some(
      ZonedDateTime.of(2024, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
    ),
    "timestampStruct" -> (
      "aTimestamp" -> ZonedDateTime.of(
        2024,
        10,
        11,
        12,
        0,
        0,
        0,
        ZoneId.of("Europe/London")
      ),
      "repeatedTimestamp" -> List(
        ZonedDateTime.of(2025, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London")),
        ZonedDateTime.of(2024, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
      ),
      "optionalTimestamp" -> Some(
        ZonedDateTime.of(2024, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
      )
    ),
    "dateStruct" -> (
      "aDate" -> LocalDate.of(2009, 8, 26),
      "repeatedDate" -> List(
        LocalDate.of(2008, 8, 26),
        LocalDate.of(2000, 6, 19)
      ),
      "optionalDate" -> Some(LocalDate.of(2008, 9, 26))
    ),
    "doubleStruct" -> (
      "aDouble" -> 1.24,
      "doubleRepeated" -> List(1.24, 3.20),
      "doubleNullable" -> Some(1.24)
    ),
    "floatStruct" -> (
      "aFloat" -> 1.24f,
      "floatRepeated" -> List(1.24f, 3.20f),
      "floatNullable" -> Some(1.24f)
    ),
    "longStruct" -> (
      "aLong" -> 11L,
      "longRepeated" -> List(21L, 11L),
      "longNullable" -> Some(31L)
    ),
    "boolStruct" -> (
      "aBool" -> false,
      "boolRepeated" -> List(false, true).sorted,
      "boolNullable" -> Some(true)
    ),
    "aDouble" -> 1.24,
    "doubleRepeated" -> List(1.24, 3.20),
    "doubleNullable" -> Some(1.24),
    "aFloat" -> 1.24f,
    "floatRepeated" -> List(1.24f, 3.20f),
    "floatNullable" -> Some(1.24f),
    "aLong" -> 11L,
    "longRepeated" -> List(21L, 11L),
    "longNullable" -> Some(31L),
    "columns" -> List(
      ("name" -> "FirstName", "type" -> "String"),
      ("name" -> "FamilyNane", "type" -> "String"),
      ("name" -> "Age", "type" -> "Int")
    ),
    "aBool" -> false,
    "boolRepeated" -> List(false, true).sorted,
    "boolNullable" -> Some(true),
    "anInt" -> 10,
    "optionalInt" -> Some(10),
    "emptyOptionalInt" -> None,
    "repeatedInt" -> List(10, 20, 30),
    "emptyRepeatedInt" -> List(),
    "struct" -> (
      "nest1" -> "ciccio3",
      "nest2" -> "ciccio4",
      "intList" -> List(1, 2, 3)
    ),
    "optionalStruct" -> Some(("nest1" -> "ciccio5", "nest2" -> "ciccio6")),
    "emptyOptionalStruct" -> None
  )

  val repeatedTypeSchema: StructType = StructType(
    List(
      "columns" -> StructType(
        List("name" -> StringType(), "type" -> StringType()),
        Repeated
      ),
      "additionalField" -> StringType(),
      "externalStruct" -> StructType(List("inner-field" -> StringType()))
    )
  )

  val repeatedTypeTuple: Tuple = Tuple3(
    "columns" -> List(
      ("type" -> "Int", "name" -> "Age"),
      ("type" -> "String", "name" -> "FamilyNane"),
      ("type" -> "String", "name" -> "FirstName")
    ),
    "additionalField" -> "Example",
    "externalStruct" -> Tuple1("inner-field" -> "StructExample")
  )

  "Listing all existing EntityTypes" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val listEntityTypes1 = EntityType(
        "ListEntityTypes1",
        Set(),
        StructType(
          List("field1" -> StringType(), "field2" -> StringType())
        ): Schema
      )

      val listEntityTypes2 = EntityType(
        "ListEntityTypes2",
        Set(),
        StructType(
          List("field1" -> StringType(), "field2" -> StringType())
        ): Schema
      )

      val testResult: IO[Either[ManagementServiceError, List[EntityType]]] =
        session.use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val (trservice, service, _, _) = getManagementServices(repository)

          (for {
            _ <- EitherT.liftF(service.create(listEntityTypes1))
            _ <- EitherT.liftF(service.create(listEntityTypes2))
            finalList <- EitherT(service.list())
          } yield finalList).value
        }

      testResult asserting {
        case Right(list) =>
          val filteredList = list.filter(e =>
            e.name.equals(listEntityTypes1.name) || e.name.equals(
              listEntityTypes2.name
            )
          )
          if (filteredList.size.equals(2)) succeed
          else fail("Expected exactly two entities but found different")

        case Left(ManagementServiceError(_)) =>
          fail("Management service error occurred")
      }
    }
  }

  "Creating an EntityType instance" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val schema: StructType = StructType(
        List(
          "organization" -> StringType(),
          "sub-organization" -> StringType(),
          "domain" -> StringType(),
          "sub-domain" -> StringType(),
          "version" -> IntType(),
          "foundation" -> DateType(),
          "timestamp" -> TimestampType(),
          "double" -> DoubleType(),
          "float" -> FloatType(),
          "aStruct" -> StructType(
            List(
              "nest1" -> StringType(),
              "nest2" -> StructType(
                List("nest3" -> StringType(), "nest4" -> StringType())
              )
            )
          )
        )
      )
      session.use(session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, service, _, _) = getManagementServices(repository)

        val entityType = EntityType("TestType", schema)

        service.create(entityType) *>
          service.read("TestType").map(_.map(_.schema))
      ) asserting (_.map(sc =>
        import cats.syntax.all.*
        import it.agilelab.dataplatformshaper.domain.model.schema.given
        sc === schema
      ) shouldBe Right(true))
    }
  }

  "Deleting an EntityType instance" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val schema: StructType = StructType(
        List(
          "organization" -> StringType(),
          "sub-organization" -> StringType(),
          "domain" -> StringType(),
          "sub-domain" -> StringType(),
          "version" -> IntType(),
          "foundation" -> DateType(),
          "timestamp" -> TimestampType(),
          "double" -> DoubleType(),
          "float" -> FloatType(),
          "aStruct" -> StructType(
            List(
              "nest1" -> StringType(),
              "nest2" -> StructType(
                List("nest3" -> StringType(), "nest4" -> StringType())
              )
            )
          )
        )
      )

      session.use(session => {
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, service, _, _) = getManagementServices(repository)

        val entityType = EntityType("TestDeleteType", schema)

        for {
          _ <- service.create(entityType)
          _ <- service.read(entityType.name)
          inCacheAfterRead <- cache.lookup(entityType.name).map(_.isDefined)
          deleteResult <- service.delete("TestDeleteType")
          readResult <- service.read("TestDeleteType")
          inCacheAfterDeleted <- cache.lookup(entityType.name).map(_.isDefined)
        } yield (
          inCacheAfterRead,
          deleteResult,
          readResult,
          inCacheAfterDeleted
        )
      }) asserting {
        case (true, Right(()), Left(_), false) => succeed
        case _ => fail("EntityType was not deleted successfully")
      }
    }
  }

  "Creating the same EntityType instance" - {
    "fails" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, service, _, _) = getManagementServices(repository)
        val entityType = EntityType(
          "FileBasedDataCollectionType",
          Set("DataCollection"),
          fileBasedDataCollectionTypeSchema
        )
        trservice.create(Trait("DataCollection", None)) *>
          service.create(entityType) *>
          service.create(entityType)
      } asserting (ret =>
        ret should matchPattern { case Left(_) =>
        }
      )
    }
  }

  "Creating an EntityType with a non-existing trait" - {
    "fails" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, service, _, _) = getManagementServices(repository)
        val entityType = EntityType(
          "TraitExample",
          Set("NonExistingTrait"),
          fileBasedDataCollectionTypeSchema
        )
        service.create(entityType)
      } asserting (ret =>
        ret should matchPattern { case Left(_) =>
        }
      )
    }
  }

  "Caching entity type definitions" - {
    "works" in {
      cache.lookup("TestType").map(_.isDefined shouldBe true)
    }
  }

  "Deleting an EntityType instance with instance referencing it as father" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val schema: StructType = StructType(
        List(
          "organization" -> StringType(),
          "sub-organization" -> StringType(),
          "domain" -> StringType(),
          "sub-domain" -> StringType(),
          "version" -> IntType(),
          "foundation" -> DateType(),
          "timestamp" -> TimestampType(),
          "double" -> DoubleType(),
          "float" -> FloatType(),
          "aStruct" -> StructType(
            List(
              "nest1" -> StringType(),
              "nest2" -> StructType(
                List("nest3" -> StringType(), "nest4" -> StringType())
              )
            )
          )
        )
      )

      session.use(session => {
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, service, _, _) = getManagementServices(repository)

        val fatherEntityType = EntityType("FatherDeleteEntityType", schema)

        val sonEntityType =
          EntityType("SonDeleteEntityType", schema, fatherEntityType)

        for {
          _ <- service.create(fatherEntityType)
          _ <- service.read(fatherEntityType.name)
          _ <- service.create(sonEntityType)
          _ <- service.read(sonEntityType.name)
          inCacheAfterRead <- cache
            .lookup(fatherEntityType.name)
            .map(_.isDefined)
          deleteResult <- service.delete("FatherDeleteEntityType")
          readResult <- service.read("FatherDeleteEntityType")
          inCacheAfterDeleted <- cache
            .lookup(fatherEntityType.name)
            .map(_.isDefined)
        } yield (
          inCacheAfterRead,
          deleteResult,
          readResult,
          inCacheAfterDeleted
        )
      }) asserting {
        case (true, Left(ManagementServiceError(_)), Right(_), true) =>
          succeed
        case x =>
          fail("EntityType was not deleted successfully")
      }
    }
  }

  "Deleting an EntityType with existing instances" - {
    "fails" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val schema: StructType = StructType(List("name" -> StringType()))

      val testResult: IO[Either[
        ManagementServiceError,
        (Unit, Either[ManagementServiceError, EntityType])
      ]] =
        session.use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val (trservice, service, ims, _) = getManagementServices(repository)

          val entityType = EntityType("DataProductType", schema)

          (for {
            _ <- EitherT.liftF(service.create(entityType))
            _ <- EitherT(ims.create("DataProductType", Tuple1("name" -> "dp1")))
            deleteResult <- EitherT(service.delete("DataProductType"))
            readResult <- EitherT.liftF(service.read("DataProductType"))
          } yield (deleteResult, readResult)).value
        }

      testResult asserting {
        case Left(ManagementServiceError(_)) => succeed
        case _                               => fail("Unexpected result")
      }
    }
  }

  "Deleting an EntityType with descendants who have instances" - {
    "fails" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val schema: StructType = StructType(List("name" -> StringType()))

      val testResult: IO[Either[
        ManagementServiceError,
        (Unit, Either[ManagementServiceError, EntityType])
      ]] =
        session.use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val (trservice, service, ims, _) = getManagementServices(repository)

          val fatherEntityType = EntityType("FatherType", schema)

          val sonEntityType = EntityType("SonType", schema, fatherEntityType)

          (for {
            _ <- EitherT.liftF(service.create(fatherEntityType))
            _ <- EitherT.liftF(service.create(sonEntityType))
            _ <- EitherT(ims.create("SonType", Tuple1("name" -> "dp1")))
            deleteResult <- EitherT(service.delete("FatherType"))
            readResult <- EitherT.liftF(service.read("FatherType"))
          } yield (deleteResult, readResult)).value
        }

      testResult asserting {
        case Left(ManagementServiceError(_)) => succeed
        case _                               => fail("Unexpected result")
      }
    }
  }

  "Creating an EntityType with a repeated struct" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use(session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, service, _, _) = getManagementServices(repository)

        val entityType =
          EntityType("RepeatedStructTestType", repeatedTypeSchema)

        service.create(entityType) *>
          service.read("RepeatedStructTestType").map(_.map(_.schema))
      ) asserting (_.map(sc =>
        import cats.syntax.all.*
        import it.agilelab.dataplatformshaper.domain.model.schema.given
        sc === repeatedTypeSchema
      ) shouldBe Right(true))
    }
  }

  "Creating an instance for an EntityType with a repeated struct and reading it" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, tservice, iservice, _) =
          getManagementServices(repository)
        (for {
          uid <- EitherT[IO, ManagementServiceError, String](
            iservice.create("RepeatedStructTestType", repeatedTypeTuple)
          )
          read <- EitherT[IO, ManagementServiceError, Entity](
            iservice.read(uid)
          )
        } yield read).value
      } asserting (entity =>
        inside(entity) { case Right(entity) =>
          val x = entity.values.toArray.map(_.asInstanceOf[(String, Any)]).toMap
          val y =
            repeatedTypeTuple.toArray.map(_.asInstanceOf[(String, Any)]).toMap
          assert(
            x.keySet === y.keySet &&
              x("additionalField") === y("additionalField") &&
              x("externalStruct") === y("externalStruct") && {
                val xcols = x("columns")
                  .asInstanceOf[List[((String, String), (String, String))]]
                  .map(
                    _.toArray
                      .map(_.asInstanceOf[(String, String)])
                      .map(_.toArray.map(_.asInstanceOf[String]).toSet)
                      .toSet
                  )
                  .toSet
                val ycols = y("columns")
                  .asInstanceOf[List[((String, String), (String, String))]]
                  .map(
                    _.toArray
                      .map(_.asInstanceOf[(String, String)])
                      .map(_.toArray.map(_.asInstanceOf[String]).toSet)
                      .toSet
                  )
                  .toSet
                xcols === ycols
              }
          )
        }
      )
    }
  }

  "Creating an instance for an EntityType that doesn't exist" - {
    "fails" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, tservice, iservice, _) =
          getManagementServices(repository)
        iservice.create(
          "MissingDataCollectionType",
          fileBasedDataCollectionTuple
        )
      } asserting (ret =>
        ret should matchPattern {
          case Left(ManagementServiceError(List(error)))
              if error.contains("does not exist") =>
        }
      )
    }
  }

  "Creating an instance for an EntityType" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, tservice, iservice, _) =
          getManagementServices(repository)
        iservice.create(
          "FileBasedDataCollectionType",
          fileBasedDataCollectionTuple
        )
      } asserting (_ should matchPattern { case Right(_) => })
    }
  }

  "Checking if an Entity instance exists" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val _ = session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, tservice, iservice, _) =
          getManagementServices(repository)
        iservice.exist("nonexistent")
      } asserting (_ should matchPattern { case Right(false) => })
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, tservice, iservice, _) =
          getManagementServices(repository)
        (for {
          uid <- EitherT[IO, ManagementServiceError, String](
            iservice.create(
              "FileBasedDataCollectionType",
              fileBasedDataCollectionTuple
            )
          )
          check <- EitherT[IO, ManagementServiceError, Boolean](
            iservice.exist(uid)
          )
        } yield check).value
      } asserting (_ should matchPattern { case Right(true) => })
    }
  }

  "Retrieving an Entity given its id" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, tservice, iservice, _) =
          getManagementServices(repository)
        (for {
          uid <- EitherT[IO, ManagementServiceError, String](
            iservice.create(
              "FileBasedDataCollectionType",
              fileBasedDataCollectionTuple
            )
          )
          read <- EitherT[IO, ManagementServiceError, Entity](
            iservice.read(uid)
          )
        } yield read).value
      } asserting (entity =>
        inside(entity) { case Right(entity) =>
          import cats.syntax.all.*
          import it.agilelab.dataplatformshaper.domain.model.schema.given
          entity.values === fileBasedDataCollectionTuple shouldBe true
        }
      )
    }
  }

  "Updating an Entity given its id" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, tservice, iservice, _) =
          getManagementServices(repository)
        (for {
          uid <- EitherT[IO, ManagementServiceError, String](
            iservice.create(
              "FileBasedDataCollectionType",
              fileBasedDataCollectionTuple
            )
          )
          _ <- EitherT[IO, ManagementServiceError, String](
            iservice.update(uid, fileBasedDataCollectionTupleForUpdate)
          )
          read <- EitherT[IO, ManagementServiceError, Entity](
            iservice.read(uid)
          )
        } yield read).value
      } asserting (entity => {
        val _ = entity should matchPattern {
          case Right(Entity(_, "FileBasedDataCollectionType", _)) =>
        }
        entity match {
          case Right(Entity(_, _, data)) =>
            import cats.syntax.all.*
            import it.agilelab.dataplatformshaper.domain.model.schema.given
            data === fileBasedDataCollectionTupleForUpdate shouldBe true
          case _ => fail("Unexpected pattern encountered")
        }
      })
    }
  }

  "Retrieving the EntityType given its name" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, service, _, _) = getManagementServices(repository)
        service.read("FileBasedDataCollectionType")
      } asserting (_.map(et =>
        import cats.syntax.all.*
        import it.agilelab.dataplatformshaper.domain.model.schema.given
        et.name === "FileBasedDataCollectionType" && et.traits === Set(
          "DataCollection"
        ) && et.baseSchema === fileBasedDataCollectionTypeSchema
      ) shouldBe Right(true))
    }
  }

  "Reading a Non-Existent Entity" - {
    "succeeds if an error is returned" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, tservice, iservice, _) =
          getManagementServices(repository)
        val nonExistentId = "non-existent-id"

        (for {
          readResult <- EitherT[IO, ManagementServiceError, Entity](
            iservice.read(nonExistentId)
          )
        } yield readResult).value
      } asserting {
        case Left(ManagementServiceError(List(error)))
            if error.contains("does not exist") =>
          succeed
        case _ =>
          fail("Expected an error for non-existent entity, but got success")
      }
    }
  }

  "Using a service when there is no connection with the knowledge graph" - {
    "fails" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "non_existing_user",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, service, _, _) = getManagementServices(repository)
        service.read("FileBasedDataCollectionType")
      }.attempt asserting (_ should matchPattern { case Left(_) => })
    }
  }

  "Inheriting from another EntityType with traits" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val commonSchema: StructType =
        StructType(List("commonString" -> StringType()))

      val schema: StructType = StructType(List("anotherString" -> StringType()))

      session.use(session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, service, _, _) = getManagementServices(repository)

        val commonEntityType = EntityType("CommonEntityType", commonSchema)

        (for {
          _ <- EitherT[IO, ManagementServiceError, Unit](
            service.create(commonEntityType)
          )
          _ <- EitherT[IO, ManagementServiceError, EntityType](
            service.read("CommonEntityType")
          )
          _ <- EitherT[IO, ManagementServiceError, Unit](
            service
              .create(EntityType("BaseEntityType", schema), "CommonEntityType")
          )
          etype <- EitherT[IO, ManagementServiceError, EntityType](
            service.read("BaseEntityType")
          )
        } yield etype).value
      ) asserting (et =>
        et shouldBe Right(
          EntityType(
            "BaseEntityType",
            schema,
            EntityType("CommonEntityType", commonSchema)
          )
        )
      )
    }
  }

  "Following the inheritance chain for an EntityType" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val schema0: Schema = StructType(List("field0" -> StringType()))

      val schema1: Schema =
        StructType(List("field1" -> StringType(), "field3" -> StringType()))

      val schema2: Schema = StructType(
        List(
          "field1" -> StringType(),
          "field2" -> StringType(),
          "field3" -> StringType(),
          "field4" -> StringType()
        )
      )

      val entityType0 = EntityType("EntityType0", schema0)

      val entityType1 = EntityType("EntityType1", schema1)

      val entityType2 = EntityType("EntityType2", schema2)

      session.use(session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, service, _, _) = getManagementServices(repository)
        (for {
          _ <- EitherT[IO, ManagementServiceError, Unit](
            service.create(entityType0)
          )
          _ <- EitherT[IO, ManagementServiceError, Unit](
            service.create(entityType1, "EntityType0")
          )
          _ <- EitherT[IO, ManagementServiceError, Unit](
            service.create(entityType2, "EntityType1")
          )
          etype <- EitherT[IO, ManagementServiceError, EntityType](
            service.read("EntityType2")
          )
        } yield etype).value
      ) asserting (et =>
        et.map(_.schema) shouldBe Right(
          StructType(
            List(
              "field0" -> StringType(),
              "field1" -> StringType(),
              "field2" -> StringType(),
              "field3" -> StringType(),
              "field4" -> StringType()
            )
          )
        )
      )
    }
  }
end OntologyL0Spec
