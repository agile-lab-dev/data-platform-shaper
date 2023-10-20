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
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.service.interpreter.TraitManagementServiceInterpreter
import org.eclipse.rdf4j.model.*
import org.eclipse.rdf4j.model.util.Values
import org.eclipse.rdf4j.model.util.Values.iri
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.multipart.{Multipart, Multiparts, Part}
import org.http4s.{EntityEncoder, Method, Request, Uri}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy

import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
import scala.util.Right

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.null"
  )
)
class OntologyL1Spec
    extends AsyncFreeSpec
    with AsyncIOSpec
    with Matchers
    with BeforeAndAfterAll:

  val graphdbContainer = new GenericContainer("ontotext/graphdb:10.3.1")

  graphdbContainer.addExposedPort(7200)
  graphdbContainer.setPortBindings(List("0.0.0.0:" + 7201 + ":" + 7200).asJava)

  override protected def beforeAll(): Unit =
    graphdbContainer.start()
    graphdbContainer.waitingFor(new HostPortWaitStrategy())
    val port = graphdbContainer.getMappedPort(7200).intValue()
    createRepository(port)
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
      val session = Session[IO]("localhost", 7201, "repo1", false)
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

  "Checking the existence of a non existing Trait" - {
    "works" in {
      val session = Session[IO]("localhost", 7201, "repo1", false)
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val trms = TraitManagementServiceInterpreter[IO](repository)
        trms.exist("NonExistentTrait")
      } asserting (res => res should be(Right(false)))
    }
  }

  "Creating a trait" - {
    "works" in {
      val session = Session[IO]("localhost", 7201, "repo1", false)
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

end OntologyL1Spec
