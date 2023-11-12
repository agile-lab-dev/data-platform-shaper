package it.agilelab.dataplatformshaper.uservice.api

import cats.effect
import cats.effect.*
import cats.effect.std.Random
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.io.file.Path
import io.circe.*
import io.circe.parser.*
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.dataplatformshaper.domain.model.NS.*
import it.agilelab.dataplatformshaper.domain.model.l0.EntityType
import it.agilelab.dataplatformshaper.uservice.definitions.{
  AttributeTypeName,
  AttributeType as OpenApiAttributeType,
  Entity as OpenApiEntity,
  EntityType as OpenApiEntityType,
  Mode as OpenApiMode,
  Trait as OpenApiTrait
}
import it.agilelab.dataplatformshaper.uservice.server.impl.Server
import it.agilelab.dataplatformshaper.uservice.{
  Client,
  CreateEntityByYamlResponse,
  CreateEntityResponse,
  ReadEntityResponse,
  ReadTypeResponse,
  LinkTraitResponse,
  UnlinkTraitResponse,
  LinkedTraitsResponse,
  LinkEntityResponse,
  LinkedEntitiesResponse,
  UnlinkEntityResponse
}
import org.eclipse.rdf4j.model.util.Values.iri
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}
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
        _ <- Resource.liftK(client.createTrait(OpenApiTrait("DataCollection")))
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

      val childrenEntityType = OpenApiEntityType(
        name = "newChildrenEntityType",
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

      val stream = fs2.io.readClassLoaderResource[IO]("entity-type.yaml")

      val resp: Resource[IO, ReadTypeResponse] = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createTypeByYaml(stream))
        resp <- Resource.liftK(client.readType("newChildrenEntityType"))
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

      val resp: Resource[IO, ReadEntityResponse] = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        id <- Resource.liftK(client.createEntityByYaml(stream).map {
          case CreateEntityByYamlResponse.Ok(id) => id
          case _                                 => ""
        })
        reResp <- Resource.liftK(client.readEntity(id))
      } yield reResp

      val rawJson: String = """
      {
        "name": "dc1"
      }
      """

      val entityJson = parse(rawJson).getOrElse(Json.Null)

      resp
        .use(resp => IO.pure(resp))
        .asserting(resp =>
          inside(resp) {
            case ReadEntityResponse
                  .Ok(OpenApiEntity(_, "DataCollectionType", json)) =>
              json should be(entityJson)
          }
        )
    }
  }

  "Linking two traits" - {
    "should create a link between trait1 and trait2 and verify the link" in {

      val trait1 = "trait1"
      val trait2 = "trait2"
      val relationship = "hasPart"

      val resp: Resource[IO, (LinkTraitResponse, LinkedTraitsResponse)] = for
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(Client.httpClient(_, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createTrait(OpenApiTrait(trait1)))
        _ <- Resource.liftK(client.createTrait(OpenApiTrait(trait2)))
        linkResp <- Resource.liftK(
          client.linkTrait(trait1, relationship, trait2)
        )
        linkedTraitsResp <- Resource.liftK(
          client.linkedTraits(trait1, relationship)
        )
      yield (linkResp, linkedTraitsResp)

      resp
        .use { case (linkResponse, linkedTraitsResponse) =>
          IO {
            linkResponse should matchPattern { case LinkTraitResponse.Ok(_) => }
            linkedTraitsResponse should matchPattern {
              case LinkedTraitsResponse.Ok(traits) if traits.contains(trait2) =>
            }
          }
        }
        .asserting(assertion => assertion)
    }
  }

  "Unlinking two traits" - {
    "should remove the link between trait1 and trait2" in {

      val trait1 = "trait3"
      val trait2 = "trait4"
      val relationship = "hasPart"

      val resp: Resource[IO, (UnlinkTraitResponse, LinkedTraitsResponse)] =
        for {
          client <- EmberClientBuilder
            .default[IO]
            .build
            .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
          _ <- Resource.liftK(client.createTrait(OpenApiTrait(trait1)))
          _ <- Resource.liftK(client.createTrait(OpenApiTrait(trait2)))
          _ <- Resource.liftK(client.linkTrait(trait1, relationship, trait2))
          unlinkResp <- Resource.liftK(
            client.unlinkTrait(trait1, relationship, trait2)
          )
          linkedTraitsResp <- Resource.liftK(
            client.linkedTraits(trait1, relationship)
          )
        } yield (unlinkResp, linkedTraitsResp)

      resp
        .use { case (unlinkResponse, linkedTraitsResponse) =>
          IO {
            unlinkResponse should matchPattern {
              case UnlinkTraitResponse.Ok(_) =>
            }
            linkedTraitsResponse should matchPattern {
              case LinkedTraitsResponse.Ok(traits)
                  if !traits.contains(trait2) =>
            }
          }
        }
        .asserting(assertion => assertion)
    }
  }

  "Linking two entities" - {
    "should create a link between instances with ids instanceId1 and instanceId2 and verify the link" in {

      val relationship = "hasPart"
      val entityType1 = OpenApiEntityType(
        name = "LinkedEntity1",
        Some(Vector("Trait1")),
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
      val entityType2 = OpenApiEntityType(
        name = "LinkedEntity2",
        Some(Vector("Trait2")),
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
      val rawJson: String =
        """
        {
          "name": "dc1"
        }
        """

      val entityJson = parse(rawJson).getOrElse(Json.Null)
      val entity1 = OpenApiEntity("", "LinkedEntity1", entityJson)
      val entity2 = OpenApiEntity("", "LinkedEntity2", entityJson)

      val resp: Resource[IO, Either[
        String,
        (LinkEntityResponse, LinkedEntitiesResponse, String)
      ]] = for
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(Client.httpClient(_, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createTrait(OpenApiTrait("Trait1")))
        _ <- Resource.liftK(client.createTrait(OpenApiTrait("Trait2")))
        _ <- Resource.liftK(client.linkTrait("Trait1", relationship, "Trait2"))
        _ <- Resource.liftK(client.createType(entityType1))
        _ <- Resource.liftK(client.createType(entityType2))
        createResp1 <- Resource.liftK(client.createEntity(entity1))
        createResp2 <- Resource.liftK(client.createEntity(entity2))
        result <- Resource.liftK {
          (createResp1, createResp2) match {
            case (CreateEntityResponse.Ok(id1), CreateEntityResponse.Ok(id2)) =>
              for {
                linkResp <- client.linkEntity(id1, relationship, id2)
                linkedEntitiesResp <- client.linkedEntities(id1, relationship)
              } yield Right((linkResp, linkedEntitiesResp, id2))
            case _ =>
              IO.pure(Left("Entity creation failed for one or both entities"))
          }
        }
      yield result

      resp
        .use {
          case Right((linkResponse, linkedEntitiesResponse, instanceId2)) =>
            IO {
              linkResponse should matchPattern {
                case LinkEntityResponse.Ok(_) =>
              }
              linkedEntitiesResponse should matchPattern {
                case LinkedEntitiesResponse.Ok(entities)
                    if entities.contains(instanceId2) =>
              }
            }
          case Left(errorMessage) =>
            IO.raiseError(new Exception(errorMessage))
        }
        .asserting(assertion => assertion)
    }
  }

  "Unlinking two entities" - {
    "should remove a link between instances with ids instanceId1 and instanceId2" in {

      val relationship = "hasPart"
      val entityType1 = OpenApiEntityType(
        name = "LinkedEntity3",
        Some(Vector("Trait3")),
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
      val entityType2 = OpenApiEntityType(
        name = "LinkedEntity4",
        Some(Vector("Trait4")),
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
      val rawJson: String =
        """
        {
          "name": "dc1"
        }
        """

      val entityJson = parse(rawJson).getOrElse(Json.Null)
      val entity1 = OpenApiEntity("", "LinkedEntity3", entityJson)
      val entity2 = OpenApiEntity("", "LinkedEntity4", entityJson)

      val resp: Resource[IO, Either[
        String,
        (UnlinkEntityResponse, LinkedEntitiesResponse, String)
      ]] = for
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(Client.httpClient(_, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createTrait(OpenApiTrait("Trait3")))
        _ <- Resource.liftK(client.createTrait(OpenApiTrait("Trait4")))
        _ <- Resource.liftK(client.linkTrait("Trait3", relationship, "Trait4"))
        _ <- Resource.liftK(client.createType(entityType1))
        _ <- Resource.liftK(client.createType(entityType2))
        createResp1 <- Resource.liftK(client.createEntity(entity1))
        createResp2 <- Resource.liftK(client.createEntity(entity2))
        result <- Resource.liftK {
          (createResp1, createResp2) match {
            case (CreateEntityResponse.Ok(id1), CreateEntityResponse.Ok(id2)) =>
              for {
                _ <- client.linkEntity(id1, relationship, id2)
                unlinkResp <- client.unlinkEntity(id1, relationship, id2)
                linkedEntitiesResp <- client.linkedEntities(id1, relationship)
              } yield Right((unlinkResp, linkedEntitiesResp, id2))
            case _ =>
              IO.pure(Left("Entity creation failed for one or both entities"))
          }
        }
      yield result

      resp
        .use {
          case Right((unlinkResponse, linkedEntitiesResponse, instanceId2)) =>
            IO {
              unlinkResponse should matchPattern {
                case UnlinkEntityResponse.Ok(_) =>
              }
              linkedEntitiesResponse should matchPattern {
                case LinkedEntitiesResponse.Ok(entities)
                    if !entities.contains(instanceId2) =>
              }
            }
          case Left(errorMessage) =>
            IO.raiseError(new Exception(errorMessage))
        }
        .asserting(assertion => assertion)
    }
  }

end ApiSpec
