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
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.model.l1.Relationship
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.*
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.multipart.{Multipart, Multiparts, Part}
import org.http4s.{EntityEncoder, Method, Request, Uri}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Inside.inside
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy

import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
import scala.util.Right

class OntologyL1Spec
    extends AsyncFreeSpec
    with AsyncIOSpec
    with Matchers
    with BeforeAndAfterAll:

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
        container.addExposedPort(8890)
        container.setPortBindings(
          List(
            "0.0.0.0:" + 7201 + ":" + 1111,
            "0.0.0.0:" + 8890 + ":" + 8890
          ).asJava
        )
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

  "Checking the existence of a non existing Trait" - {
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
        val trms = TraitManagementServiceInterpreter[IO](repository)
        trms.exist("NonExistentTrait")
      } asserting (res => res should be(Right(false)))
    }
  }

  "Creating a trait" - {
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
        val trms = TraitManagementServiceInterpreter[IO](repository)
        (for {
          _ <- EitherT(trms.create("ANewTrait", None))
          res <- EitherT(trms.exist("ANewTrait"))
        } yield res).value
      } asserting (res => res should be(Right(true)))
    }
  }

  "Linking a trait to another trait" - {
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
        val trms = TraitManagementServiceInterpreter[IO](repository)
        (for {
          _ <- EitherT(trms.create("DataProductComponent", None))
          _ <- EitherT(trms.create("OutputPort", Some("DataProductComponent")))
          _ <- EitherT(trms.create("DataProduct", None))
          _ <- EitherT(
            trms
              .link("DataProduct", Relationship.hasPart, "DataProductComponent")
          )
          res <- EitherT(trms.linked("DataProduct", Relationship.hasPart))
        } yield res).value
      } asserting (res => res should be(Right(List("DataProductComponent"))))
    }
  }

  "Link with relationship hasPart different instances of entity types" - {
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
        val tms = TypeManagementServiceInterpreter[IO](trservice)
        val ims = InstanceManagementServiceInterpreter[IO](tms)
        (for {
          _ <- EitherT(
            tms.create(
              EntityType(
                "DataProductType",
                Set("DataProduct"),
                StructType(List("name" -> StringType())): Schema
              )
            )
          )
          _ <- EitherT(
            tms.create(
              EntityType(
                "OutputPortType",
                Set("OutputPort"),
                StructType(List("name" -> StringType())): Schema
              )
            )
          )
          dp <- EitherT(ims.create("DataProductType", Tuple1("name" -> "dp1")))
          op1 <- EitherT(ims.create("OutputPortType", Tuple1("name" -> "op1")))
          op2 <- EitherT(ims.create("OutputPortType", Tuple1("name" -> "op2")))
          _ <- EitherT(ims.link(dp, Relationship.hasPart, op1))
          _ <- EitherT(ims.link(dp, Relationship.hasPart, op2))
          ids <- EitherT(ims.linked(dp, Relationship.hasPart))
        } yield (ids, op1, op2)).value
      } asserting (res =>
        inside(res) {
          case Right(entity) =>
            entity(0) shouldBe List(entity(1), entity(2))
          case Left(_) =>
            true shouldBe false
        }
      )
    }
  }

  "Deleting an instance with linked instances should trigger an error" - {
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
        val tms = TypeManagementServiceInterpreter[IO](trservice)
        val ims = InstanceManagementServiceInterpreter[IO](tms)
        (for {
          dp <- EitherT(ims.create("DataProductType", Tuple1("name" -> "dp1")))
          op1 <- EitherT(ims.create("OutputPortType", Tuple1("name" -> "op1")))
          op2 <- EitherT(ims.create("OutputPortType", Tuple1("name" -> "op2")))
          _ <- EitherT(ims.link(dp, Relationship.hasPart, op1))
          _ <- EitherT(ims.link(dp, Relationship.hasPart, op2))
          le <- EitherT(ims.delete(dp))
        } yield le).value
      } asserting (res =>
        inside(res) {
          case Right(_) =>
            false shouldBe true
          case Left(error) =>
            error should matchPattern {
              case InstanceHasLinkedInstancesError(_) =>
            }
        }
      )
    }
  }

  "Unlinking two traits that have associated linked instances should trigger an error" - {
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
        trservice.unlink("DataProduct", Relationship.hasPart, "OutputPort")
      } asserting (res =>
        res should matchPattern {
          case Left(
                TraitsHaveLinkedInstancesError("DataProduct", "OutputPort")
              ) =>
        }
      )
    }
  }

  "Link with relationship hasPart two instances and one of them is not related to the proper trait" - {
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
        val tms = TypeManagementServiceInterpreter[IO](trservice)
        val ims = InstanceManagementServiceInterpreter[IO](tms)
        (for {
          _ <- EitherT(
            tms.create(
              EntityType(
                "AnotherType",
                StructType(List("name" -> StringType())): Schema
              )
            )
          )
          dp <- EitherT(ims.create("DataProductType", Tuple1("name" -> "dp2")))
          at1 <- EitherT(ims.create("AnotherType", Tuple1("name" -> "at1")))
          res <- EitherT(ims.link(dp, Relationship.hasPart, at1))
        } yield res).value
      } asserting (res =>
        res should matchPattern { case Left(InvalidLinkType(_, "hasPart", _)) =>
        }
      )
    }
  }

end OntologyL1Spec
