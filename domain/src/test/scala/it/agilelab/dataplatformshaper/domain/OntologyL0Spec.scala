package it.agilelab.dataplatformshaper.domain

import cats.data.EitherT
import cats.effect.std.Random
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref}
import fs2.io.file.Path
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.dataplatformshaper.domain.model.NS.*
import it.agilelab.dataplatformshaper.domain.model.l0
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import org.datatools.bigdatatypes.basictypes.SqlType
import org.eclipse.rdf4j.model.*
import org.eclipse.rdf4j.model.util.Values
import org.eclipse.rdf4j.model.util.Values.iri
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}
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
    "scalafix:DisableSyntax.null",
    "scalafix:DisableSyntax.asInstanceOf",
    "scalafix:DisableSyntax.isInstanceOf",
    "scalafix:DisableSyntax.=="
  )
)
class OntologyL0Spec
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

  val graphdbContainer =
    graphdbType match
      case "graphdb" =>
        val container = new GenericContainer("ontotext/graphdb:10.3.1")
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
        val model1 = Rio.parse(
          Thread.currentThread.getContextClassLoader
            .getResourceAsStream("dp-ontology-l0.ttl"),
          ns.getName,
          RDFFormat.TURTLE,
          L0
        )
        val model2 = Rio.parse(
          Thread.currentThread.getContextClassLoader
            .getResourceAsStream("dp-ontology-l1.ttl"),
          ns.getName,
          RDFFormat.TURTLE,
          L1
        )
        val statements1 = model1.getStatements(null, null, null, iri(ns, "L0"))
        val statements2 = model2.getStatements(null, null, null, iri(ns, "L1"))
        repository.removeAndInsertStatements(
          statements1.asScala.toList ++ statements2.asScala.toList
        ) *> repository.evaluateQuery(
          s"""
               |prefix ns: <${ns.getName}>
               |select ?p ?o {
               | ns:EntityType ?p ?o .
               |}
               |""".stripMargin
        )
      } asserting (_.toList.length shouldBe 1)
    }
  }
//TODO: Make lists order dependent
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
      )
    )
  )

  val fileBasedDataCollectionTuple = (
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
  )

  val fileBasedDataCollectionTupleForUpdate = (
    "organization" -> "HR",
    "sub-organization" -> "Any",
    "domain" -> "Registrations",
    "sub-domain" -> "People",
    "foundation" -> LocalDate.of(1969, 5, 25),
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
      ZonedDateTime.of(2025, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London")),
      ZonedDateTime.of(2024, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
    ),
    "optionalTimestamp" -> Some(
      ZonedDateTime.of(2024, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
    ),
    "timestampStruct" -> (
      "timestamp" -> ZonedDateTime.of(
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
      "date" -> LocalDate.of(2009, 8, 26),
      "repeatedDate" -> List(
        LocalDate.of(2008, 8, 26),
        LocalDate.of(2000, 6, 19)
      ),
      "optionalDate" -> Some(LocalDate.of(2008, 9, 26))
    ),
    "doubleStruct" -> (
      "double" -> 1.24,
      "doubleRepeated" -> List(1.24, 3.20),
      "doubleNullable" -> Some(1.24)
    ),
    "floatStruct" -> (
      "float" -> 1.24f,
      "floatRepeated" -> List(1.24f, 3.20f),
      "floatNullable" -> Some(1.24f)
    ),
    "longStruct" -> (
      "long" -> 11L,
      "longRepeated" -> List(21L, 11L),
      "longNullable" -> Some(31L)
    ),
    "boolStruct" -> (
      "bool" -> false,
      "boolRepeated" -> List(false, true).sorted,
      "boolNullable" -> Some(true)
    ),
    "double" -> 1.24,
    "doubleRepeated" -> List(1.24, 3.20),
    "doubleNullable" -> Some(1.24),
    "float" -> 1.24f,
    "floatRepeated" -> List(1.24f, 3.20f),
    "floatNullable" -> Some(1.24f),
    "long" -> 11L,
    "longRepeated" -> List(21L, 11L),
    "longNullable" -> Some(31L),
    "bool" -> false,
    "boolRepeated" -> List(false, true).sorted,
    "boolNullable" -> Some(true),
    "int" -> 10,
    "optionalInt" -> Some(10),
    "emptyOptionalInt" -> None,
    "repeatedInt" -> List(10, 20, 30),
    "emptyRepeatedInt" -> List(),
    "struct" -> (
      "nest1" -> "ciccio3",
      "nest2" -> "ciccio4",
      "intList" -> List(1, 2, 3)
    ),
    "optionalStruct" -> Some(
      (
        "nest1" -> "ciccio5",
        "nest2" -> "ciccio6"
      )
    ),
    "emptyOptionalStruct" -> None,
  )

  val repeatedTypeSchema: StructType = StructType(
    List(
      "columns" -> StructType(
        List(
          "name" -> StringType(),
          "type" -> StringType(),
          "date" -> DateType(),
          "timestamp" -> TimestampDataType()
        ),
        Repeated
      ),
      "additionalField" -> StringType(),
      "externalStruct" -> StructType(
        List("inner-field" -> StringType())
      )
    )
  )

  val repeatedTypeTuple = Tuple3(
    "columns" -> List(
      (
        "name" -> "FirstName",
        "type" -> "String",
        "date" -> LocalDate.of(2009, 8, 26),
        "timestamp" -> ZonedDateTime
          .of(2025, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
      ),
      (
        "name" -> "FamilyNane",
        "type" -> "String",
        "date" -> LocalDate.of(2000, 6, 19),
        "timestamp" -> ZonedDateTime
          .of(2024, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
      ),
      (
        "name" -> "Age",
        "type" -> "Int",
        "date" -> LocalDate.of(1996, 11, 24),
        "timestamp" -> ZonedDateTime
          .of(2023, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London"))
      )
    ),
    "additionalField" -> "Example",
    "externalStruct" -> Tuple1("inner-field" -> "StructExample")
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
      val schema: StructType = StructType(
        List(
          "organization" -> StringType(),
          "sub-organization" -> StringType(),
          "domain" -> StringType(),
          "sub-domain" -> StringType(),
          "version" -> IntType(),
          "foundation" -> DateType(),
          "timestamp" -> TimestampDataType(),
          "double" -> DoubleType(),
          "float" -> FloatType(),
          "aStruct" -> StructType(
            List(
              "nest1" -> StringType(),
              "nest2" -> StructType(
                List(
                  "nest3" -> StringType(),
                  "nest4" -> StringType()
                )
              )
            )
          )
        )
      )
      session.use(session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val trservice = new TraitManagementServiceInterpreter[IO](repository)
        val service = new TypeManagementServiceInterpreter[IO](trservice)

        val entityType = EntityType(
          "TestType",
          schema
        )

        service.create(entityType) *>
          service.read("TestType").map(_.map(_.schema))
      ) asserting (sc => sc shouldBe Right(schema))
    }
  }

  "Creating the same EntityType instance" - {
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
        val trservice = new TraitManagementServiceInterpreter[IO](repository)
        val service = new TypeManagementServiceInterpreter[IO](trservice)
        val entityType = l0.EntityType(
          "FileBasedDataCollectionType",
          Set("DataCollection"),
          fileBasedDataCollectionTypeSchema
        )
        trservice.create("DataCollection", None) *>
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
        val service = new TypeManagementServiceInterpreter[IO](trservice)
        val entityType = l0.EntityType(
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

    "Caching entity type definitions" - {
      "works" in {
        cache.get.asserting(_.size shouldBe 1)
      }
    }

    "Creating an EntityType with a repeated struct" - {
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
        session.use(session =>
          val repository = Rdf4jKnowledgeGraph[IO](session)
          val trservice = new TraitManagementServiceInterpreter[IO](repository)
          val service = new TypeManagementServiceInterpreter[IO](trservice)

          val entityType = EntityType(
            "RepeatedStructTestType",
            repeatedTypeSchema
          )

          service.create(entityType) *>
            service.read("RepeatedStructTestType").map(_.map(_.schema))
        ) asserting (sc => sc shouldBe Right(repeatedTypeSchema))
      }
    }

    "Creating an instance for an EntityType with a repeated struct and reading it" - {
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
          (for {
            uid <- EitherT[IO, ManagementServiceError, String](
              iservice.create(
                "RepeatedStructTestType",
                repeatedTypeTuple
              )
            )
            read <- EitherT[IO, ManagementServiceError, Entity](
              iservice.read(uid)
            )
          } yield read).value
        } asserting (entity =>
          inside(entity) { case Right(entity) =>
            assert(
              tupleToJsonChecked(
                entity.values,
                repeatedTypeSchema
              )
                .equals(
                  tupleToJsonChecked(
                    repeatedTypeTuple,
                    repeatedTypeSchema
                  )
                )
            )
          }
        )
      }
    }

    "Creating an instance for an EntityType that doesn't exist" - {
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
          val trservice = new TraitManagementServiceInterpreter[IO](repository)
          val tservice = new TypeManagementServiceInterpreter[IO](trservice)
          val iservice = new InstanceManagementServiceInterpreter[IO](tservice)
          iservice.create(
            "MissingDataCollectionType",
            fileBasedDataCollectionTuple
          )
        } asserting (ret =>
          ret should matchPattern { case Left(_) =>
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

    "Checking if an Entity instance exists" - {
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
          iservice.exist("nonexistent")
        } asserting (_ should matchPattern { case Right(false) => })
        session.use { session =>
          val repository = Rdf4jKnowledgeGraph[IO](session)
          val trservice = new TraitManagementServiceInterpreter[IO](repository)
          val tservice = new TypeManagementServiceInterpreter[IO](trservice)
          val iservice = new InstanceManagementServiceInterpreter[IO](tservice)
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
            assert(
              tupleToJsonChecked(
                entity.values,
                fileBasedDataCollectionTypeSchema
              )
                .equals(
                  tupleToJsonChecked(
                    fileBasedDataCollectionTuple,
                    fileBasedDataCollectionTypeSchema
                  )
                )
            )
          }
        )
      }
    }

    "Updating an Entity given its id" - {
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
          (for {
            uid <- EitherT[IO, ManagementServiceError, String](
              iservice.create(
                "FileBasedDataCollectionType",
                fileBasedDataCollectionTuple
              )
            )
            _ <- EitherT[IO, ManagementServiceError, String](
              iservice.update(
                uid,
                fileBasedDataCollectionTupleForUpdate
              )
            )
            read <- EitherT[IO, ManagementServiceError, Entity](
              iservice.read(uid)
            )
          } yield read).value
        } asserting (entity => {
          entity should matchPattern {
            case Right(Entity(_, "FileBasedDataCollectionType", _)) =>
          }
          entity match {
            case Right(Entity(_, _, data)) =>
              val x =
                tupleToJsonChecked(data, fileBasedDataCollectionTypeSchema)
              val y = tupleToJsonChecked(
                fileBasedDataCollectionTupleForUpdate,
                fileBasedDataCollectionTypeSchema
              )
              x shouldBe y
            case _ => fail("Unexpected pattern encountered")
          }
        })
      }
    }

    "Retrieving the EntityType given its name" - {
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
          val service = new TypeManagementServiceInterpreter[IO](trservice)
          service.read("FileBasedDataCollectionType")
        } asserting (_ shouldBe Right(
          l0.EntityType(
            "FileBasedDataCollectionType",
            Set("DataCollection"),
            fileBasedDataCollectionTypeSchema
          )
        ))
      }
    }

    "Reading a Non-Existent Entity" - {
      "succeeds if an error is returned" in {
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
          val nonExistentId = "non-existent-id"

          (for {
            readResult <- EitherT[IO, ManagementServiceError, Entity](
              iservice.read(nonExistentId)
            )
          } yield readResult).value
        } asserting (result => {
          result match {
            case Left(_) => succeed
            case Right(_) =>
              fail("Expected an error for non-existent entity, but got success")
          }
        })
      }
    }

    "Using a service when there is no connection with the knowledge graph" - {
      "fails" in {
        val session = Session[IO](
          graphdbType,
          "localhost",
          7210,
          "dba",
          "mysecret",
          "repo1",
          false
        )
        session.use { session =>
          val repository = Rdf4jKnowledgeGraph[IO](session)
          val trservice = new TraitManagementServiceInterpreter[IO](repository)
          val service = new TypeManagementServiceInterpreter[IO](trservice)
          service.read("FileBasedDataCollectionType")
        }.attempt asserting (_ should matchPattern { case Left(_) => })
      }
    }

    "Inheriting from another EntityType with traits" - {
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
        val commonSchema: StructType = StructType(
          List(
            "commonString" -> StringType()
          )
        )

        val schema: StructType = StructType(
          List(
            "anotherString" -> StringType()
          )
        )

        session.use(session =>
          val repository = Rdf4jKnowledgeGraph[IO](session)
          val trservice = new TraitManagementServiceInterpreter[IO](repository)
          val service = new TypeManagementServiceInterpreter[IO](trservice)

          val commonEntityType = l0.EntityType(
            "CommonEntityType",
            commonSchema
          )

          (for {
            _ <- EitherT[IO, ManagementServiceError, Unit](
              service.create(commonEntityType)
            )
            _ <- EitherT[IO, ManagementServiceError, EntityType](
              service.read("CommonEntityType")
            )
            _ <- EitherT[IO, ManagementServiceError, Unit](
              service.create(
                EntityType("BaseEntityType", schema),
                "CommonEntityType"
              )
            )
            etype <- EitherT[IO, ManagementServiceError, EntityType](
              service.read("BaseEntityType")
            )
          } yield etype).value
        ) asserting (et =>
          et shouldBe Right(
            l0.EntityType(
              "BaseEntityType",
              schema,
              l0.EntityType("CommonEntityType", commonSchema)
            )
          )
        )
      }
    }

    "Following the inheritance chain for an EntityType" - {
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
        val schema0: Schema = StructType(
          List(
            "field0" -> StringType()
          )
        )

        val schema1: Schema = StructType(
          List(
            "field1" -> StringType(),
            "field3" -> StringType()
          )
        )

        val schema2: Schema = StructType(
          List(
            "field1" -> StringType(),
            "field2" -> StringType(),
            "field3" -> StringType(),
            "field4" -> StringType()
          )
        )

        val entityType0 = EntityType("EntityType0", schema0)

        val entityType1 = l0.EntityType("EntityType1", schema1)

        val entityType2 = l0.EntityType("EntityType2", schema2)

        session.use(session =>
          val repository = Rdf4jKnowledgeGraph[IO](session)
          val trservice = new TraitManagementServiceInterpreter[IO](repository)
          val service = new TypeManagementServiceInterpreter[IO](trservice)
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
  }
end OntologyL0Spec
