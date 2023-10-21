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
import org.eclipse.rdf4j.model.*
import org.eclipse.rdf4j.model.util.Values
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

import scala.collection.immutable.List
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
import scala.util.Right

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.null"
  )
)
class OntologyL0Spec
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

  val fileBasedDataCollectionTypeSchema: StructType = StructType(
    List(
      "organization" -> StringType(),
      "sub-organization" -> StringType(),
      "domain" -> StringType(),
      "sub-domain" -> StringType(),
      "labels" -> StringType(Repeated),
      "string" -> StringType(),
      "optionalString" -> StringType(Nullable),
      "emptyOptionalString" -> StringType(Nullable),
      "repeatedString" -> StringType(Repeated),
      "emptyRepeatedString" -> StringType(Repeated),
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
    "labels" -> List("label1", "label2", "label3"),
    "string" -> "str",
    "optionalString" -> Some("str"),
    "emptyOptionalString" -> None,
    "repeatedString" -> List("str1", "str2", "str3"),
    "emptyRepeatedString" -> List(),
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

  "Creating an EntityType instance" - {
    "works" in {
      val session = Session[IO]("localhost", 7201, "repo1", false)
      val schema: StructType = StructType(
        List(
          "organization" -> StringType(),
          "sub-organization" -> StringType(),
          "domain" -> StringType(),
          "sub-domain" -> StringType(),
          "version" -> IntType(),
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
        val service = new TypeManagementServiceInterpreter[IO](repository)

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
      val session = Session[IO]("localhost", 7201, "repo1", false)
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val service = new TypeManagementServiceInterpreter[IO](repository)
        val trservice = new TraitManagementServiceInterpreter[IO](repository)
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

  "Caching entity type definitions" - {
    "works" in {
      cache.get.asserting(_.size shouldBe 1)
    }
  }

  "Creating an instance for an EntityType that doesn't exist" - {
    "fails" in {
      val session = Session[IO]("localhost", 7201, "repo1", false)
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val tservice = new TypeManagementServiceInterpreter[IO](repository)
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
      val session = Session[IO]("localhost", 7201, "repo1", false)
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val tservice = new TypeManagementServiceInterpreter[IO](repository)
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
      val session = Session[IO]("localhost", 7201, "repo1", false)
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val tservice = new TypeManagementServiceInterpreter[IO](repository)
        val iservice = new InstanceManagementServiceInterpreter[IO](tservice)
        iservice.exist("nonexistent")
      } asserting (_ should matchPattern { case Right(false) => })
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val tservice = new TypeManagementServiceInterpreter[IO](repository)
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
      val session = Session[IO]("localhost", 7201, "repo1", false)
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val tservice = new TypeManagementServiceInterpreter[IO](repository)
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
          entity.values should be(fileBasedDataCollectionTuple)
        }
      )
    }
  }

  "Updating an Entity given its id" - {
    "works" in {
      val session = Session[IO]("localhost", 7201, "repo1", false)
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val tservice = new TypeManagementServiceInterpreter[IO](repository)
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
              (
                "organization" -> "HR",
                "sub-organization" -> "Any",
                "domain" -> "Registrations",
                "sub-domain" -> "People",
                "labels" -> List("label1", "label2", "label3"),
                "string" -> "str",
                "optionalString" -> Some("str"),
                "emptyOptionalString" -> None,
                "repeatedString" -> List("str1", "str2", "str3"),
                "emptyRepeatedString" -> List(),
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
            )
          )
          read <- EitherT[IO, ManagementServiceError, Entity](
            iservice.read(uid)
          )
        } yield read).value
      } asserting (entity =>
        entity should matchPattern {
          case Right(
                Entity(
                  _,
                  "FileBasedDataCollectionType",
                  (
                    "organization" -> "HR",
                    "sub-organization" -> "Any",
                    "domain" -> "Registrations",
                    "sub-domain" -> "People",
                    "labels" -> List("label1", "label2", "label3"),
                    "string" -> "str",
                    "optionalString" -> Some("str"),
                    "emptyOptionalString" -> None,
                    "repeatedString" -> List("str1", "str2", "str3"),
                    "emptyRepeatedString" -> List(),
                    "int" -> 10,
                    "optionalInt" -> Some(10),
                    "emptyOptionalInt" -> None,
                    "repeatedInt" -> List(10, 20, 30),
                    "emptyRepeatedInt" -> List(),
                    "struct" -> (
                      "nest1" -> "ciccio3", "nest2" -> "ciccio4",
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
                )
              ) =>
        }
      )
    }
  }

  "Retrieving the EntityType given its name" - {
    "works" in {
      val session = Session[IO]("localhost", 7201, "repo1", false)
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val service = new TypeManagementServiceInterpreter[IO](repository)
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

  "Using a service when there is no connection with the knowledge graph" - {
    "fails" in {
      val session = Session[IO]("localhost", 7210, "repo1", false)
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val service = new TypeManagementServiceInterpreter[IO](repository)
        service.read("FileBasedDataCollectionType")
      }.attempt asserting (_ should matchPattern { case Left(_) => })
    }
  }

  "Inheriting from another EntityType with traits" - {
    "works" in {
      val session = Session[IO]("localhost", 7201, "repo1", false)
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
        val service = new TypeManagementServiceInterpreter[IO](repository)

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
      val session = Session[IO]("localhost", 7201, "repo1", false)

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
        val service = new TypeManagementServiceInterpreter[IO](repository)
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
