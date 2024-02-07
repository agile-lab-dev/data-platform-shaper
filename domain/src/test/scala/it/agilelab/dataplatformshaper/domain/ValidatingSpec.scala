package it.agilelab.dataplatformshaper.domain

import io.circe.*
import io.circe.parser.*
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import org.scalactic.Equality
import org.scalatest.BeforeAndAfterAll
import cats.effect.std.Random
import cats.effect.testing.scalatest.AsyncIOSpec
import org.http4s.multipart.{Multipart, Multiparts, Part}
import org.http4s.{EntityEncoder, Method, Request, Uri}
import fs2.io.file.Path
import org.http4s.ember.client.EmberClientBuilder
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{Rdf4jKnowledgeGraph, Session}
import it.agilelab.dataplatformshaper.domain.service.interpreter.{TraitManagementServiceInterpreter, TypeManagementServiceInterpreter}
import it.agilelab.dataplatformshaper.domain.model.l0
import it.agilelab.dataplatformshaper.domain.model.l0.*

import scala.jdk.CollectionConverters.*
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy
import cats.effect.{IO, Ref}

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.language.{dynamics, implicitConversions}

class ValidatingSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers with BeforeAndAfterAll:

  val graphdbType = "graphdb"

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


  "Unfolding a tuple conform to a schema" - {
    "should work" in {
      val res = cueValidate(schema, tuple)
      res match {
        case Right(_) => succeed
        case Left(errors) => fail(s"Validation failed with errors: $errors")
      }
    }
  }

  "Creating the same EntityType instance" - {
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

end ValidatingSpec
