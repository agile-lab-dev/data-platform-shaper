package it.agilelab.witboost.ontology.manager.uservice.api

import cats.effect
import cats.effect.*
import cats.effect.std.Random
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.io.file.Path
import io.circe.*
import io.circe.parser.*
import it.agilelab.witboost.ontology.manager.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.witboost.ontology.manager.domain.model.NS.*
import it.agilelab.witboost.ontology.manager.domain.model.l0.EntityType
import it.agilelab.witboost.ontology.manager.uservice.definitions.{
  AttributeTypeName,
  AttributeType as OpenApiAttributeType,
  Entity as OpenApiEntity,
  EntityType as OpenApiEntityType,
  Mode as OpenApiMode
}
import it.agilelab.witboost.ontology.manager.uservice.server.impl.Server
import it.agilelab.witboost.ontology.manager.uservice.{
  Client,
  CreateEntityByYamlResponse,
  CreateEntityResponse,
  CreateTypeByYamlResponse,
  ReadTypeResponse
}
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

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.null",
    "scalafix:DisableSyntax.var"
  )
)
class ApiSpec
    extends AsyncFreeSpec
    with AsyncIOSpec
    with Matchers
    with BeforeAndAfterAll:

  val graphdbContainer = new GenericContainer("ontotext/graphdb:10.3.1")

  graphdbContainer.addExposedPort(7200)
  graphdbContainer.setPortBindings(List("0.0.0.0:" + 7202 + ":" + 7200).asJava)

  var server: Option[FiberIO[Nothing]] = None

  override protected def beforeAll(): Unit =
    graphdbContainer.start()
    graphdbContainer.waitingFor(new HostPortWaitStrategy())
    val port = graphdbContainer.getMappedPort(7200).intValue()
    createRepository(port)
    loadBaseOntologies()
    server = Some(createServer())
    Thread.sleep(1000)
  end beforeAll

  override protected def afterAll(): Unit =
    server.foreach(_.cancel.unsafeRunSync())
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
                  Path("uservice/src/test/resources/repo-config.ttl")
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
    val session = Session[IO]("localhost", 7202, "repo1", false)
    session
      .use { session =>
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
        )
      }
      .unsafeRunSync()
  end loadBaseOntologies

  def createServer(): FiberIO[Nothing] =
    val session: Session = Session.getSession(
      "127.0.0.1",
      7202,
      "repo1",
      false
    )
    val typeCache: Ref[IO, Map[String, EntityType]] =
      Ref[IO].of(Map.empty[String, EntityType]).unsafeRunSync()
    Server
      .server[IO](session, typeCache)
      .use(_ => IO.never)
      .start
      .unsafeRunSync()
  end createServer

  "Creating a user defined type" - {
    "works" in {

      val fatherEntityType = OpenApiEntityType(
        "father",
        None,
        Vector(
          OpenApiAttributeType("id", AttributeTypeName.String, None, None)
        ),
        None
      )

      val childrenEntityType = OpenApiEntityType(
        name = "childrenEntityType",
        Some(Vector("DataCollection")),
        Vector(
          OpenApiAttributeType(
            "name",
            AttributeTypeName.String,
            Some(OpenApiMode.Required),
            None
          )
        ),
        Some("father")
      )

      val resp: Resource[IO, ReadTypeResponse] = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createType(fatherEntityType))
        _ <- Resource.liftK(client.createType(childrenEntityType))
        resp <- Resource.liftK(client.readType("childrenEntityType"))
      } yield resp

      resp
        .use(resp => IO.pure(resp))
        .asserting(resp =>
          resp should be(
            ReadTypeResponse.Ok(
              childrenEntityType
            )
          )
        )
    }
  }

  "Creating a user defined type using a YAML file" - {
    "works" in {
      /*
      val childrenEntityType = OpenApiEntityType(
        name = "childrenEntityType",
        Some(Vector("DataCollection")),
        Vector(
          OpenApiAttributeType(
            "name",
            AttributeTypeName.String,
            Some(OpenApiMode.Required),
            None
          )
        ),
        Some("father")
      )
       */
      val stream = fs2.io.readClassLoaderResource[IO]("entity-type.yaml")

      val resp: Resource[IO, CreateTypeByYamlResponse] = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        resp <- Resource.liftK(client.createTypeByYaml(stream))
        // resp <- Resource.liftK(client.readType("newChildrenEntityType"))
      } yield resp

      resp
        .use(resp => IO.pure(resp))
        .asserting(resp =>
          resp should be(
            CreateTypeByYamlResponse.Ok(
              "OK"
            )
          )
        )
    }
  }

  "Creating a user defined type instance" - {
    "works" in {

      val entityType = OpenApiEntityType(
        name = "DataCollectionType",
        Some(Vector("DataCollection")),
        Vector(
          OpenApiAttributeType(
            "name",
            AttributeTypeName.String,
            Some(OpenApiMode.Required),
            None
          )
        ),
        None
      )

      val rawJson: String = """
      {
        "name": "dc1"
      }
      """

      val entityJson = parse(rawJson).getOrElse(Json.Null)

      val entity = OpenApiEntity("", "DataCollectionType", entityJson)

      val resp: Resource[IO, CreateEntityResponse] = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createType(entityType))
        resp <- Resource.liftK(client.createEntity(entity))
      } yield resp

      resp
        .use(resp => IO.pure(resp))
        .asserting(resp =>
          resp should matchPattern { case CreateEntityResponse.Ok(_) =>
          }
        )
    }
  }

  "Creating a user defined type instance using a YAML file" - {
    "works" in {

      val stream = fs2.io.readClassLoaderResource[IO]("entity.yaml")

      val resp: Resource[IO, CreateEntityByYamlResponse] = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        resp <- Resource.liftK(client.createEntityByYaml(stream))
      } yield resp

      resp
        .use(resp => IO.pure(resp))
        .asserting(resp =>
          resp should matchPattern { case CreateEntityByYamlResponse.Ok(_) =>
          }
        )
    }
  }

end ApiSpec
