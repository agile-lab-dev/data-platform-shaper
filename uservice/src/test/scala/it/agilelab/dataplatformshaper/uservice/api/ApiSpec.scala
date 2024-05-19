package it.agilelab.dataplatformshaper.uservice.api

import cats.effect
import cats.effect.*
import cats.effect.std.Random
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.io.file.Path
import io.chrisdavenport.mules.caffeine.CaffeineCache
import io.chrisdavenport.mules.{Cache, TimeSpec}
import io.circe.*
import io.circe.parser.*
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.dataplatformshaper.domain.model.EntityType
import it.agilelab.dataplatformshaper.uservice.definitions.Mode.members.Required
import it.agilelab.dataplatformshaper.uservice.{
  Client,
  CreateEntityByYamlResponse,
  CreateEntityResponse,
  DeleteTraitResponse,
  LinkEntityResponse,
  LinkTraitResponse,
  LinkedEntitiesResponse,
  LinkedTraitsResponse,
  ListEntitiesByIdsResponse,
  ListEntitiesResponse,
  ListTraitsResponse,
  ListTypesResponse,
  ReadEntityResponse,
  ReadMappingResponse,
  ReadTypeResponse,
  UnlinkEntityResponse,
  UnlinkTraitResponse,
  UpdateEntityByYamlResponse,
  UpdateEntityResponse,
  UpdateTypeConstraintsResponse
}
import it.agilelab.dataplatformshaper.uservice.definitions.{
  AttributeTypeName,
  MappingDefinition,
  MappingKey,
  ValidationError,
  AttributeType as OpenApiAttributeType,
  Entity as OpenApiEntity,
  EntityType as OpenApiEntityType,
  Mode as OpenApiMode,
  Trait as OpenApiTrait
}
import it.agilelab.dataplatformshaper.uservice.server.impl.Server
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.multipart.{Multipart, Multiparts, Part}
import org.http4s.{EntityEncoder, Method, Request, Uri}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Inside.inside
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps

@SuppressWarnings(Array("scalafix:DisableSyntax.var"))
class ApiSpec
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
        container.setPortBindings(List("0.0.0.0:" + 7202 + ":" + 7200).asJava)
        container
      case "virtuoso" =>
        val container = GenericContainer(
          "openlink/virtuoso-opensource-7:latest"
        )
        container.withEnv("DBA_PASSWORD", "mysecret")
        container.addExposedPort(1111)
        container.setPortBindings(List("0.0.0.0:" + 7202 + ":" + 1111).asJava)
        container
    end match

  var server: Option[FiberIO[Nothing]] = None

  override protected def beforeAll(): Unit =
    graphdbContainer.start()
    graphdbContainer.waitingFor(HostPortWaitStrategy())
    if graphdbType === "graphdb" then
      val port = graphdbContainer.getMappedPort(7200).intValue()
      createRepository(port)
    end if
    loadBaseOntologies()
    server = Some(createServer())
    Thread.sleep(1000)
  end beforeAll

  override protected def afterAll(): Unit =
    // Thread.sleep(1000000000)
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
    val session = Session[IO](
      graphdbType,
      "localhost",
      7202,
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

  def createServer(): FiberIO[Nothing] =
    val session = Session.getSession(
      graphdbType,
      "localhost",
      7202,
      "dba",
      "mysecret",
      "repo1",
      false
    )
    val typeCache: Cache[IO, String, EntityType] = CaffeineCache
      .build[IO, String, EntityType](
        Some(TimeSpec.unsafeFromDuration(1.second)),
        None,
        None
      )
      .unsafeRunSync()
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

      val resp = for {
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
          resp should be(ReadTypeResponse.Ok(childrenEntityType))
        )
    }
  }

  "Creating a mapping" - {
    "works" in {
      val createSourceEntityType = OpenApiEntityType(
        "CreateSourceEntityType",
        Some(Vector("MappingSource")),
        Vector(
          OpenApiAttributeType("field1", AttributeTypeName.String, None, None),
          OpenApiAttributeType("field2", AttributeTypeName.String, None, None)
        ),
        None
      )
      val createTargetEntityType = OpenApiEntityType(
        "CreateTargetEntityType",
        Some(Vector("MappingTarget")),
        Vector(
          OpenApiAttributeType("field1", AttributeTypeName.String, None, None),
          OpenApiAttributeType("field2", AttributeTypeName.String, None, None)
        ),
        None
      )

      val mapperTuple: String =
        """
          {
            "field1": "instance.get('field1')",
            "field2": "instance.get('field2')"
          }
          """
      val resp = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createType(createSourceEntityType))
        _ <- Resource.liftK(client.createType(createTargetEntityType))
        jsonMapperTuple <- Resource.eval(IO.fromEither(parse(mapperTuple)))
        mappingDefinition = MappingDefinition(
          MappingKey(
            "createMapping",
            createSourceEntityType.name,
            createTargetEntityType.name
          ),
          jsonMapperTuple
        )
        _ <- Resource.liftK(client.createMapping(mappingDefinition))
        resp <- Resource.liftK(
          client.readMapping(
            mappingDefinition.mappingKey.mappingName,
            mappingDefinition.mappingKey.sourceEntityTypeName,
            mappingDefinition.mappingKey.targetEntityTypeName
          )
        )
      } yield (resp, mappingDefinition)

      resp
        .use { response =>
          IO.pure(response)
        }
        .asserting {
          case (
                ReadMappingResponse.Ok(mappingDefinition),
                createdMappingDefinition
              ) =>
            mappingDefinition shouldEqual createdMappingDefinition
          case _ => fail("Update not successful")
        }
    }
  }

  "Updating a mapping" - {
    "works" in {
      val updateSourceEntityType = OpenApiEntityType(
        "UpdateSourceEntityType",
        Some(Vector("MappingSource")),
        Vector(
          OpenApiAttributeType("field1", AttributeTypeName.String, None, None),
          OpenApiAttributeType("field2", AttributeTypeName.String, None, None)
        ),
        None
      )
      val updateTargetEntityType = OpenApiEntityType(
        "UpdateTargetEntityType",
        Some(Vector("MappingTarget")),
        Vector(
          OpenApiAttributeType("field1", AttributeTypeName.String, None, None),
          OpenApiAttributeType("field2", AttributeTypeName.String, None, None)
        ),
        None
      )

      val mapperTuple: String =
        """
        {
          "field1": "instance.get('field1')",
          "field2": "instance.get('field2')"
        }
        """
      val updatedMapperTuple: String =
        """
        {
          "field1": "instance.get('field2')",
          "field2": "instance.get('field1')"
        }
        """
      val resp = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createType(updateSourceEntityType))
        _ <- Resource.liftK(client.createType(updateTargetEntityType))
        jsonMapperTuple <- Resource.eval(IO.fromEither(parse(mapperTuple)))
        jsonUpdatedMapperTuple <- Resource.eval(
          IO.fromEither(parse(updatedMapperTuple))
        )
        initialMappingDefinition = MappingDefinition(
          MappingKey(
            "updateMapping",
            updateSourceEntityType.name,
            updateTargetEntityType.name
          ),
          jsonMapperTuple
        )
        updatedMappingDefinition = MappingDefinition(
          MappingKey(
            "updateMapping",
            updateSourceEntityType.name,
            updateTargetEntityType.name
          ),
          jsonUpdatedMapperTuple
        )
        _ <- Resource.liftK(client.createMapping(initialMappingDefinition))
        _ <- Resource.liftK(client.updateMapping(updatedMappingDefinition))
        resp <- Resource.liftK(
          client.readMapping(
            updatedMappingDefinition.mappingKey.mappingName,
            updatedMappingDefinition.mappingKey.sourceEntityTypeName,
            updatedMappingDefinition.mappingKey.targetEntityTypeName
          )
        )
      } yield (resp, updatedMappingDefinition)

      resp
        .use { response =>
          IO.pure(response)
        }
        .asserting {
          case (
                ReadMappingResponse.Ok(mappingDefinition),
                updatedMappingDefinition
              ) =>
            mappingDefinition shouldEqual updatedMappingDefinition
          case _ => fail("Update not successful")
        }
    }
  }

  "Deleting a mapping" - {
    "works" in {
      val deleteSourceEntityType = OpenApiEntityType(
        "DeleteSourceEntityType",
        Some(Vector("MappingSource")),
        Vector(
          OpenApiAttributeType("field1", AttributeTypeName.String, None, None),
          OpenApiAttributeType("field2", AttributeTypeName.String, None, None)
        ),
        None
      )
      val deleteTargetEntityType = OpenApiEntityType(
        "DeleteTargetEntityType",
        Some(Vector("MappingTarget")),
        Vector(
          OpenApiAttributeType("field1", AttributeTypeName.String, None, None),
          OpenApiAttributeType("field2", AttributeTypeName.String, None, None)
        ),
        None
      )

      val mapperTuple: String =
        """
          {
            "field1": "instance.get('field1')",
            "field2": "instance.get('field2')"
          }
          """
      val resp = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createType(deleteSourceEntityType))
        _ <- Resource.liftK(client.createType(deleteTargetEntityType))
        jsonMapperTuple <- Resource.eval(IO.fromEither(parse(mapperTuple)))
        mappingDefinition = MappingDefinition(
          MappingKey(
            "deleteMapping",
            deleteSourceEntityType.name,
            deleteTargetEntityType.name
          ),
          jsonMapperTuple
        )
        _ <- Resource.liftK(client.createMapping(mappingDefinition))
        _ <- Resource.liftK(
          client.deleteMapping(
            mappingDefinition.mappingKey.mappingName,
            mappingDefinition.mappingKey.sourceEntityTypeName,
            mappingDefinition.mappingKey.targetEntityTypeName
          )
        )
        resp <- Resource.liftK(
          client.readMapping(
            mappingDefinition.mappingKey.mappingName,
            mappingDefinition.mappingKey.sourceEntityTypeName,
            mappingDefinition.mappingKey.targetEntityTypeName
          )
        )
      } yield resp

      resp
        .use { response =>
          IO.pure(response)
        }
        .asserting {
          case ReadMappingResponse.BadRequest(_) => succeed
          case _ => fail("Update not successful")
        }
    }
  }

  "Deleting a user defined type" - {
    "works" in {

      val deleteEntityType = OpenApiEntityType(
        "deleteType",
        None,
        Vector(
          OpenApiAttributeType("id", AttributeTypeName.String, None, None)
        ),
        None
      )

      val resp = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createType(deleteEntityType))
        _ <- Resource.liftK(client.deleteType("deleteType"))
        resp <- Resource.liftK(client.readType("deleteType"))
      } yield resp
      resp
        .use(resp => IO.pure(resp))
        .asserting(resp =>
          resp should be(
            ReadTypeResponse.BadRequest(
              ValidationError(
                Vector("The EntityType deleteType does not exist")
              )
            )
          )
        )
    }
  }

  "Listing all the user defined types" - {
    "works" in {
      val entityType1 = OpenApiEntityType(
        "listEntities1",
        Some(Vector()),
        Vector(
          OpenApiAttributeType("id", AttributeTypeName.Integer, None, None)
        ),
        None
      )

      val entityType2 = OpenApiEntityType(
        "listEntities2",
        Some(Vector()),
        Vector(
          OpenApiAttributeType("id", AttributeTypeName.Integer, None, None)
        ),
        None
      )

      val resp = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createType(entityType1))
        _ <- Resource.liftK(client.createType(entityType2))
        finalTypes <- Resource.liftK(client.listTypes())
      } yield finalTypes

      resp
        .use(resp => IO.pure(resp))
        .asserting {
          case ListTypesResponse.Ok(types) =>
            val names = types.map(_.name)
            assert(
              names.contains(entityType1.name) && names
                .contains(entityType2.name),
              s"Expected entity types not found. Found: ${names.mkString(", ")}"
            )
          case _ =>
            fail(
              "Expected Ok response with entity types, but received error response"
            )
        }
    }
  }

  "Listing all the traits" - {
    "works" in {
      val trait1 = "listTrait1"
      val trait2 = "listTrait2"
      val resp = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createTrait(OpenApiTrait(trait1)))
        _ <- Resource.liftK(client.createTrait(OpenApiTrait(trait2)))
        finalTraits <- Resource.liftK(client.listTraits())
      } yield finalTraits

      resp
        .use(resp => IO.pure(resp))
        .asserting {
          case ListTraitsResponse.Ok(traits) =>
            assert(
              traits.contains(trait1) && traits.contains(trait2),
              s"Expected traits not found. Found: ${traits.mkString(", ")}"
            )
          case _ =>
            fail(
              "Expected Ok response with trait list, but received error response"
            )
        }
    }
  }

  "Updating a user defined type constraints" - {
    "works" in {

      val entityType = OpenApiEntityType(
        "entityTypeConstrained",
        Some(Vector()),
        Vector(
          OpenApiAttributeType("id", AttributeTypeName.Integer, None, None)
        ),
        None
      )

      val entityTypeConstrained = OpenApiEntityType(
        "entityTypeConstrained",
        Some(Vector()),
        Vector(
          OpenApiAttributeType(
            "id",
            AttributeTypeName.Integer,
            Some(Required),
            Some(">= 5")
          )
        ),
        None
      )

      val resp = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createType(entityType))
        _ <- Resource.liftK(client.updateTypeConstraints(entityTypeConstrained))
        resp <- Resource.liftK(client.readType("entityTypeConstrained"))
      } yield resp

      resp
        .use(resp => IO.pure(resp))
        .asserting(resp =>
          resp should be(ReadTypeResponse.Ok(entityTypeConstrained))
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
          resp should be(ReadTypeResponse.Ok(childrenEntityType))
        )
    }
  }

  "Creating a mapping using a YAML file" - {
    "works" in {

      val sourceEntityType = OpenApiEntityType(
        name = "yamlMapSourceType",
        Some(Vector("MappingSource", "yamlSource")),
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

      val targetEntityType = OpenApiEntityType(
        name = "yamlMapTargetType",
        Some(Vector("MappingTarget")),
        Vector(
          OpenApiAttributeType(
            "name",
            AttributeTypeName.String,
            Some(OpenApiMode.Required),
            None
          ),
          OpenApiAttributeType(
            "additionalParameter",
            AttributeTypeName.String,
            Some(OpenApiMode.Required),
            None
          )
        ),
        None
      )

      val linkedEntityType = OpenApiEntityType(
        name = "yamlMapLinkedType",
        Some(Vector("yamlLinked")),
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

      val sourceRawJson: String =
        """
          {
            "name": "Jim"
          }
        """

      val sourceEntityJson = parse(sourceRawJson).getOrElse(Json.Null)

      val linkedRawJson: String =
        """
          {
            "name": "Milton"
          }
        """

      val linkedEntityJson = parse(linkedRawJson).getOrElse(Json.Null)

      val stream = fs2.io.readClassLoaderResource[IO]("mappingDefinition.yaml")

      val resp: Resource[IO, Json] = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))

        _ <- Resource.liftK(
          client.createTrait(OpenApiTrait("yamlSource", None))
        )
        _ <- Resource.liftK(
          client.createTrait(OpenApiTrait("yamlLinked", None))
        )
        _ <- Resource.liftK(
          client.linkTrait("yamlSource", "hasPart", "yamlLinked")
        )

        _ <- Resource.liftK(client.createType(sourceEntityType))
        _ <- Resource.liftK(client.createType(targetEntityType))
        _ <- Resource.liftK(client.createType(linkedEntityType))

        resp <- Resource.liftK(client.createMappingByYaml(stream))
        createEntityResp <- Resource.liftK(
          client.createEntity(
            OpenApiEntity("", sourceEntityType.name, sourceEntityJson)
          )
        )
        _ <- Resource.liftK(
          client.createEntity(
            OpenApiEntity("", linkedEntityType.name, linkedEntityJson)
          )
        )
        sourceId = createEntityResp.fold(identity, _ => "", _ => "")
        res <- Resource.liftK(client.createMappedInstances(sourceId))
        mappedInstances <- Resource.liftK(client.readMappedInstances(sourceId))
        instanceJson = mappedInstances.fold(
          mi => mi.head.targetEntity.values,
          _ => Json.Null,
          _ => Json.Null
        )
      } yield instanceJson

      resp
        .use(resp => IO.pure(resp))
        .asserting {
          case instanceJson
              if instanceJson.hcursor
                .downField("additionalParameter")
                .focus
                .contains(Json.fromString("Milton")) =>
            succeed
          case _ =>
            fail(
              "Expected the target instance to have 'additionalParameter' with value 'Milton'"
            )
        }
    }
  }

  "Updating a user defined type constraints using a YAML file" - {
    "works" in {

      val constrainedEntityType = OpenApiEntityType(
        name = "constrainedEntityType",
        Some(Vector()),
        Vector(
          OpenApiAttributeType(
            "age",
            AttributeTypeName.Long,
            Some(OpenApiMode.Required),
            Some(">=1")
          )
        ),
        None
      )

      val stream =
        fs2.io.readClassLoaderResource[IO]("initial_entity-type.yaml")

      val updateStream =
        fs2.io.readClassLoaderResource[IO]("updated_entity-type.yaml")

      val resp: Resource[IO, ReadTypeResponse] = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createTypeByYaml(stream))
        _ <- Resource.liftK(client.updateTypeConstraintsByYaml(updateStream))
        resp <- Resource.liftK(client.readType("constrainedEntityType"))
      } yield resp

      resp
        .use(resp => IO.pure(resp))
        .asserting(resp =>
          resp should be(ReadTypeResponse.Ok(constrainedEntityType))
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

  "Creating a user defined type instance with a json attribute" - {
    "works" in {

      val entityType = OpenApiEntityType(
        name = "JsonCollectionType",
        Some(Vector("DataCollection")),
        Vector(
          OpenApiAttributeType(
            "json",
            AttributeTypeName.Json,
            Some(OpenApiMode.Required),
            None
          )
        ),
        None
      )

      val rawJson: String =
        """
        {
          "json": {"name": "Olivia Davis", "age": 31, "city": "Houston"},
          "repeatedJson": [{"name": "William Johnson", "age": 28, "city": "New Orleans"},
          {"name": "Sophia Anderson", "age": 36, "city": "Denver"}]
        }
        """

      val entityJson = parse(rawJson).getOrElse(Json.Null)

      val entity = OpenApiEntity("", "JsonCollectionType", entityJson)

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

  "Listing the ids of user defined type instances" - {
    "works" in {
      val resp = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        ids <- Resource.liftK(
          client.listEntitiesByIds("DataCollectionType", "", Some(1))
        )
      } yield ids

      resp
        .use(resp => IO.pure(resp))
        .asserting(resp =>
          inside(resp) { case ListEntitiesByIdsResponse.Ok(value) =>
            value.size should be(1)
          }
        )
    }
  }

  "Listing user defined type instances" - {
    "works" in {
      val resp = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
        ids <- Resource.liftK(client.listEntities("DataCollectionType", ""))
      } yield ids

      resp
        .use(resp => IO.pure(resp))
        .asserting(resp =>
          inside(resp) { case ListEntitiesResponse.Ok(value) =>
            value.size should be(2)
          }
        )
    }
  }

  "Deleting a trait" - {
    "should delete an existing trait" in {

      val trait1 = "deleteTrait"

      val resp: Resource[IO, DeleteTraitResponse] = for
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(Client.httpClient(_, "http://127.0.0.1:8093"))
        _ <- Resource.liftK(client.createTrait(OpenApiTrait(trait1)))
        deleteTraitResp <- Resource.liftK(client.deleteTrait(trait1))
      yield deleteTraitResp

      resp.use(resp => IO.pure(resp)).asserting { response =>
        response should matchPattern { case DeleteTraitResponse.Ok(_) =>
        }
      }
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
            val _ = linkResponse should matchPattern {
              case LinkTraitResponse.Ok(_) =>
            }
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
            val _ = unlinkResponse should matchPattern {
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
              val _ = linkResponse should matchPattern {
                case LinkEntityResponse.Ok(_) =>
              }
              linkedEntitiesResponse should matchPattern {
                case LinkedEntitiesResponse.Ok(entities)
                    if entities.contains(instanceId2) =>
              }
            }
          case Left(errorMessage) =>
            IO.raiseError(Exception(errorMessage))
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
              val _ = unlinkResponse should matchPattern {
                case UnlinkEntityResponse.Ok(_) =>
              }
              linkedEntitiesResponse should matchPattern {
                case LinkedEntitiesResponse.Ok(entities)
                    if !entities.contains(instanceId2) =>
              }
            }
          case Left(errorMessage) =>
            IO.raiseError(Exception(errorMessage))
        }
        .asserting(assertion => assertion)
    }
  }

  "Deleting a user defined type instance" - {
    "should delete a user defined type instance with given id" in {

      val entityType = OpenApiEntityType(
        name = "TypeForDeletion",
        None,
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

      val entity = OpenApiEntity("", "TypeForDeletion", entityJson)

      val resp: Resource[IO, Either[CreateEntityResponse, ReadEntityResponse]] =
        for {
          client <- EmberClientBuilder
            .default[IO]
            .build
            .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
          _ <- Resource.liftK(client.createType(entityType))
          createEntityResp <- Resource.liftK(client.createEntity(entity))
          finalResp <- createEntityResp match {
            case CreateEntityResponse.Ok(id) =>
              Resource.liftK(for {
                _ <- client.deleteEntity(id)
                readEntityResp <- client.readEntity(id)
              } yield Right(readEntityResp))
            case _ =>
              Resource
                .pure[IO, Either[CreateEntityResponse, ReadEntityResponse]](
                  Left(
                    CreateEntityResponse.BadRequest(
                      ValidationError(Vector("Failed to create entity"))
                    )
                  )
                )
          }
        } yield finalResp

      resp.use(resp => IO.pure(resp)).asserting { response =>
        response should matchPattern {
          case Right(ReadEntityResponse.BadRequest(_)) =>
        }
      }
    }
  }

  "Updating a user-defined type instance" - {
    "should update a user-defined type instance with given id and values" in {

      val entityType = OpenApiEntityType(
        name = "TypeForUpdate",
        None,
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

      val rawJsonCreate: String =
        """
        {
          "name": "initialName"
        }
        """

      val entityJsonCreate = parse(rawJsonCreate).getOrElse(Json.Null)

      val entity = OpenApiEntity("", "TypeForUpdate", entityJsonCreate)

      val rawJsonUpdate: String =
        """
        {
          "name": "updatedName"
        }
        """

      val entityUpdateJsonCreate = parse(rawJsonUpdate).getOrElse(Json.Null)
      val updatedEntity =
        OpenApiEntity("", "TypeForUpdate", entityUpdateJsonCreate)

      val resp: Resource[IO, Either[UpdateEntityResponse, ReadEntityResponse]] =
        for {
          client <- EmberClientBuilder
            .default[IO]
            .build
            .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))
          _ <- Resource.liftK(client.createType(entityType))
          createEntityResp <- Resource.liftK(client.createEntity(entity))
          finalResp <- createEntityResp match {
            case CreateEntityResponse.Ok(id) =>
              Resource.liftK(for {
                _ <- client.updateEntity(id, updatedEntity)
                readEntityResp <- client.readEntity(id)
              } yield Right(readEntityResp))
            case _ =>
              Resource
                .pure[IO, Either[UpdateEntityResponse, ReadEntityResponse]](
                  Left(
                    UpdateEntityResponse.BadRequest(
                      ValidationError(
                        Vector("Failed to create entity for update")
                      )
                    )
                  )
                )
          }
        } yield finalResp

      resp.use(resp => IO.pure(resp)).asserting { response =>
        response should matchPattern {
          case Right(ReadEntityResponse.Ok(entity))
              if entity.values.equals(updatedEntity.values) =>
        }
      }
    }
  }

  "Updating a user defined type instance using a YAML file" - {
    "works" in {

      val createStream =
        fs2.io.readClassLoaderResource[IO]("initial_entity.yaml")
      val updateStream =
        fs2.io.readClassLoaderResource[IO]("updated_entity.yaml")

      val rawJson: String =
        """
      {
        "name": "updated_entity"
      }
      """

      val updatedEntityJson = parse(rawJson).getOrElse(Json.Null)

      val testFlow: Resource[IO, ReadEntityResponse] = for {
        client <- EmberClientBuilder
          .default[IO]
          .build
          .map(client => Client.httpClient(client, "http://127.0.0.1:8093"))

        entityId <- Resource.liftK(client.createEntityByYaml(createStream).map {
          case CreateEntityByYamlResponse.Ok(id) => id
          case _ => fail("Initial entity creation failed")
        })

        _ <- Resource.liftK(
          client.updateEntityByYaml(entityId, updateStream).map {
            case UpdateEntityByYamlResponse.Ok(_) => ()
            case _ => fail("Entity update failed")
          }
        )

        readResponse <- Resource.liftK(client.readEntity(entityId))

      } yield readResponse

      testFlow.use { readResponse =>
        IO.pure(readResponse)
          .asserting { resp =>
            inside(resp) {
              case ReadEntityResponse.Ok(
                    OpenApiEntity(_, "DataCollectionType", json)
                  ) =>
                json should be(updatedEntityJson)
            }
          }
      }
    }
  }

end ApiSpec
