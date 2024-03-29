package it.agilelab.dataplatformshaper.domain

import cats.effect.IO
import cats.effect.std.Random
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.io.file.Path
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.multipart.{Multipart, Multiparts, Part}
import org.http4s.{EntityEncoder, Method, Request, Uri}
import org.scalactic.Equality
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy

import scala.collection.immutable.List
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps

class CommonSpec
    extends AsyncFreeSpec
    with AsyncIOSpec
    with Matchers
    with BeforeAndAfterAll:

  val graphdbType = "virtuoso"

  val graphdbContainer: GenericContainer[Nothing] =
    graphdbType match
      case "graphdb" =>
        val container = GenericContainer("ontotext/graphdb:10.6.0")
        container.addExposedPort(7200)
        container.setPortBindings(List("0.0.0.0:" + 7201 + ":" + 7200).asJava)
        container
      case "virtuoso" =>
        val container = GenericContainer(
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
    graphdbContainer.waitingFor(HostPortWaitStrategy())
    if graphdbType === "graphdb" then
      val port = graphdbContainer.getMappedPort(7200).intValue()
      createRepository(port)
    end if
    loadBaseOntologies()
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

  def loadBaseOntologies(): Unit =
    val session = Session[IO](
      graphdbType,
      "localhost",
      7201,
      "dba",
      "mysecret",
      "repo1",
      false
    )
    session
      .use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        repository.loadBaseOntologies()
      }
      .unsafeRunSync()
  end loadBaseOntologies
end CommonSpec
