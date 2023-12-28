package it.agilelab.dataplatformshaper.domain

import cats.effect.std.Random
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref}
import fs2.io.file.Path
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.dataplatformshaper.domain.model.l0
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.model.schema.{*, given}
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import org.datatools.bigdatatypes.basictypes.SqlType
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.multipart.{Multipart, Multiparts, Part}
import org.http4s.{EntityEncoder, Method, Request, Uri}
import org.scalactic.Equality
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Inside.inside
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.collection.immutable.List
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
import scala.util.Right

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.asInstanceOf",
    "scalafix:DisableSyntax.isInstanceOf",
    "scalafix:DisableSyntax.=="
  )
)
class OntologyL0SearchSpec
    extends AsyncFreeSpec
    with AsyncIOSpec
    with Matchers
    with BeforeAndAfterAll:

  val graphdbType = "graphdb"

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
      val c1: Map[String, SqlType] = x.records.toMap
      val c2: Map[String, SqlType] = y.asInstanceOf[StructType].records.toMap
      val ret = c1.foldLeft(true)((b, p) => b && c2(p(0)) === p(1))
      ret
    end areEqual
  end given

  val graphdbContainer: GenericContainer[Nothing] =
    graphdbType match
      case "graphdb" =>
        val container = new GenericContainer("ontotext/graphdb:10.5.0")
        container.addExposedPort(7200)
        container.setPortBindings(List("0.0.0.0:" + 7201 + ":" + 7200).asJava)
        container
      case "virtuoso" =>
        val container = new GenericContainer(
          "openlink/virtuoso-opensource-7:latest"
        )
        container.withEnv("DBA_PASSWORD", "mysecret")
        container.addExposedPort(1111)
        container.setPortBindings(List("0.0.0.0:" + 7201 + ":" + 1111).asJava)
        container
    end match

  override protected def beforeAll(): Unit =
    graphdbContainer.start()
    graphdbContainer.waitingFor(new HostPortWaitStrategy())
    if graphdbType === "graphdb" then
      val port = graphdbContainer.getMappedPort(7200).intValue()
      createRepository(port)
    end if
  end beforeAll

  override protected def afterAll(): Unit =
    // Thread.sleep(10000000)
    graphdbContainer.stop()
  end afterAll

  private def createRepository(port: Int): Unit =
    val multiparts = Random
      .scalaUtilRandom[IO]
      .map(Multiparts.fromRandom[IO])
      .syncStep(Int.MaxValue)
      .unsafeRunSync()
      .toOption
      .get

    val run: IO[Multipart[IO]] = EmberClientBuilder
      .default[IO]
      .build
      .use { client =>
        multiparts
          .multipart(
            Vector(
              Part
                .fileData[IO](
                  "config",
                  Path("domain/src/test/resources/repo-config.ttl")
                )
            )
          )
          .flatTap { multipart =>
            val entity = EntityEncoder[IO, Multipart[IO]].toEntity(multipart)
            val body = entity.body
            val request = Request(
              method = Method.POST,
              uri = Uri
                .unsafeFromString(s"http://localhost:$port/rest/repositories"),
              body = body,
              headers = multipart.headers
            )
            client.expect[String](request)
          }
      }

    val _ = run.unsafeRunSync()
  end createRepository

  given cache: Ref[IO, Map[String, EntityType]] =
    Ref[IO].of(Map.empty[String, EntityType]).unsafeRunSync()

  "Loading the base ontology" - {
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
        repository.loadBaseOntologies()
      } asserting (_ => true shouldBe true)
    }
  }

  val fileBasedDataCollectionTypeSchema: StructType = StructType(
    List(
      "organization" -> StringType(),
      "sub-organization" -> StringType(),
      "domain" -> StringType(),
      "sub-domain" -> StringType(),
      "foundation" -> DateType(),
      "labels" -> StringType(Repeated),
      "string" -> StringType(),
      "optionalString" -> StringType(Nullable),
      "emptyOptionalString" -> StringType(Nullable),
      "repeatedString" -> StringType(Repeated),
      "emptyRepeatedString" -> StringType(Repeated),
      "optionalDate" -> DateType(Nullable),
      "emptyOptionalDate" -> DateType(Nullable),
      "repeatedDate" -> DateType(Repeated),
      "emptyRepeatedDate" -> DateType(Repeated),
      "timestamp" -> TimestampDataType(Required),
      "repeatedTimestamp" -> TimestampDataType(Repeated),
      "optionalTimestamp" -> TimestampDataType(Nullable),
      "timestampStruct" -> StructType(
        List(
          "timestamp" -> TimestampDataType(Required),
          "repeatedTimestamp" -> TimestampDataType(Repeated),
          "optionalTimestamp" -> TimestampDataType(Nullable)
        )
      ),
      "dateStruct" -> StructType(
        List(
          "date" -> DateType(Required),
          "repeatedDate" -> DateType(Repeated),
          "optionalDate" -> DateType(Nullable)
        )
      ),
      "doubleStruct" -> StructType(
        List(
          "double" -> DoubleType(),
          "doubleRepeated" -> DoubleType(Repeated),
          "doubleNullable" -> DoubleType(Nullable)
        )
      ),
      "floatStruct" -> StructType(
        List(
          "float" -> FloatType(),
          "floatRepeated" -> FloatType(Repeated),
          "floatNullable" -> FloatType(Nullable)
        )
      ),
      "longStruct" -> StructType(
        List(
          "long" -> LongType(),
          "longRepeated" -> LongType(Repeated),
          "longNullable" -> LongType(Nullable)
        )
      ),
      "boolStruct" -> StructType(
        List(
          "bool" -> BooleanType(),
          "boolRepeated" -> BooleanType(Repeated),
          "boolNullable" -> BooleanType(Nullable)
        )
      ),
      "double" -> DoubleType(),
      "doubleRepeated" -> DoubleType(Repeated),
      "doubleNullable" -> DoubleType(Nullable),
      "float" -> FloatType(),
      "floatRepeated" -> FloatType(Repeated),
      "floatNullable" -> FloatType(Nullable),
      "long" -> LongType(Required),
      "longRepeated" -> LongType(Repeated),
      "longNullable" -> LongType(Nullable),
      "bool" -> BooleanType(),
      "boolRepeated" -> BooleanType(Repeated),
      "boolNullable" -> BooleanType(Nullable),
      "int" -> IntType(),
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
        List(
          "nest1" -> StringType(),
          "nest2" -> StringType()
        ),
        Nullable
      ),
      "emptyOptionalStruct" -> StructType(
        List(
          "nest1" -> StringType(),
          "nest2" -> StringType()
        ),
        Nullable
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

  val fileBasedDataCollectionTuple: Tuple = (
    "organization" -> "HR",
    "sub-organization" -> "Any",
    "domain" -> "Registrations",
    "sub-domain" -> "People",
    "foundation" -> LocalDate.of(2008, 8, 26),
    "labels" -> List("label1", "label2", "label3"),
    "string" -> "str",
    "optionalString" -> Some("str"),
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
    "repeatedTimestamp" -> List(
      ZonedDateTime.of(2023, 10, 12, 12, 0, 0, 0, ZoneId.of("Europe/London")),
      ZonedDateTime.of(2024, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
    ),
    "optionalTimestamp" -> Some(
      ZonedDateTime.of(2023, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
    ),
    "timestampStruct" -> (
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
      "date" -> LocalDate.of(2009, 8, 26),
      "repeatedDate" -> List(
        LocalDate.of(2009, 8, 26),
        LocalDate.of(2000, 6, 19)
      ),
      "optionalDate" -> Some(LocalDate.of(2008, 9, 26))
    ),
    "doubleStruct" -> (
      "double" -> 1.23,
      "doubleRepeated" -> List(1.23, 3.21),
      "doubleNullable" -> Some(1.23)
    ),
    "floatStruct" -> (
      "float" -> 1.23f,
      "floatRepeated" -> List(1.23f, 3.21f),
      "floatNullable" -> Some(1.23f)
    ),
    "longStruct" -> (
      "long" -> 10L,
      "longRepeated" -> List(10L, 20L),
      "longNullable" -> Some(30L)
    ),
    "boolStruct" -> (
      "bool" -> true,
      "boolRepeated" -> List(true, false).sorted,
      "boolNullable" -> Some(true)
    ),
    "double" -> 1.23,
    "doubleRepeated" -> List(1.23, 3.21),
    "doubleNullable" -> Some(1.23),
    "float" -> 1.23f,
    "floatRepeated" -> List(1.23f, 3.21f),
    "floatNullable" -> Some(1.23f),
    "long" -> 10L,
    "longRepeated" -> List(10L, 20L),
    "longNullable" -> Some(30L),
    "bool" -> true,
    "boolRepeated" -> List(true, false).sorted,
    "boolNullable" -> Some(true),
    "int" -> 10,
    "optionalInt" -> Some(10),
    "emptyOptionalInt" -> None,
    "repeatedInt" -> List(10, 20, 30),
    "emptyRepeatedInt" -> List(),
    "struct" -> (
      "nest1" -> "ciccio1",
      "nest2" -> "ciccio2",
      "intList" -> List(1, 2, 3)
    ),
    "optionalStruct" -> Some(
      (
        "nest1" -> "ciccio1",
        "nest2" -> "ciccio2"
      )
    ),
    "emptyOptionalStruct" -> None,
    "columns" -> List(
      (
        "type" -> "Int",
        "name" -> "Age"
      ),
      (
        "type" -> "String",
        "name" -> "FamilyNane"
      ),
      (
        "type" -> "String",
        "name" -> "FirstName"
      )
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
        val trservice = new TraitManagementServiceInterpreter[IO](repository)
        val service = new TypeManagementServiceInterpreter[IO](trservice)
        val entityType = l0.EntityType(
          "FileBasedDataCollectionType",
          Set("DataCollection"),
          fileBasedDataCollectionTypeSchema
        )
        trservice.create("DataCollection", None) *>
          service.create(entityType) *>
          service.read("FileBasedDataCollectionType")
      } asserting (ret =>
        inside(ret) { case Right(entity) =>
          entity.name shouldBe "FileBasedDataCollectionType"
          entity.traits shouldBe Set("DataCollection")
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
        val trservice = new TraitManagementServiceInterpreter[IO](repository)
        val tservice = new TypeManagementServiceInterpreter[IO](trservice)
        val iservice = new InstanceManagementServiceInterpreter[IO](tservice)
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
        val trservice = new TraitManagementServiceInterpreter[IO](repository)
        val tservice = new TypeManagementServiceInterpreter[IO](trservice)
        val iservice = new InstanceManagementServiceInterpreter[IO](tservice)
        val entityType = "FileBasedDataCollectionType"

        val predicate =
          " longStruct / longRepeated <= 50 AND organization LIKE 'H' AND organization <> 'HN' "

        iservice.list(
          entityType,
          predicate,
          returnEntities = true
        )

      } asserting (resp =>
        inside(resp) { case Right(list) =>
          list.head match
            case entity: Entity =>
              assert(list.size === 1)
              (entity.values: DynamicTuple).longStruct
                .longRepeated(0)
                .value[Long] < 50 shouldBe (true)
              (entity.values: DynamicTuple).longStruct
                .longRepeated(1)
                .value[Long] < 50 shouldBe (true)
              (entity.values: DynamicTuple).organization
                .value[String] shouldBe "HR"
            case _: String =>
              assert(false)
          end match
        }
      )
    }
  }

end OntologyL0SearchSpec
