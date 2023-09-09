package it.agilelab.witboost.ontology.manager.domain

import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Ref}
import cats.syntax.all.*
import it.agilelab.witboost.ontology.manager.domain.knowledgegraph.interpreter.{Rdf4jKnowledgeGraph, Session}
import it.agilelab.witboost.ontology.manager.domain.model.NS.*
import it.agilelab.witboost.ontology.manager.domain.model.l0.*
import it.agilelab.witboost.ontology.manager.domain.model.l1.*
import it.agilelab.witboost.ontology.manager.domain.model.schema.*
import it.agilelab.witboost.ontology.manager.domain.service.ManagementServiceError
import it.agilelab.witboost.ontology.manager.domain.service.interpreter.{InstanceManagementServiceInterpreter, TypeManagementServiceInterpreter}
import org.eclipse.rdf4j.model.util.Values.iri
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import scala.jdk.CollectionConverters.*

@SuppressWarnings(Array("org.wartremover.warts.Null"))
object TestMain1 extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    val session = Session[IO]("localhost", 7200, "repo1", false)

    val loadOntology = session.use { session =>
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

    def createEntityType(using cache: Ref[IO, Map[String, EntityType]]) =
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val service = new TypeManagementServiceInterpreter[IO](repository)
        (for {
          _ <- EitherT[IO, ManagementServiceError, Unit](
            service.create(
              EntityType("CommonType", StructType(List("id" -> StringType())))
            )
          )
          father <- EitherT[IO, ManagementServiceError, EntityType](
            service.read("CommonType")
          )
          _ <- EitherT[IO, ManagementServiceError, Unit](
            service.create(
              EntityType(
                "DataCollectionType",
                Set(Versionable, DataProduct),
                StructType(
                  List(
                    "organization" -> StringType(),
                    "sub-organization" -> StringType(),
                    "domain" -> StringType(),
                    "sub-domain" -> StringType(),
                    "additional-info" -> StructType(
                      List(
                        "info1" -> StringType(),
                        "info2" -> StringType()
                      )
                    )
                  )
                ): Schema,
                father
              )
            )
          )
        } yield ()).value
      }

    def instancesCreation(using cache: Ref[IO, Map[String, EntityType]]) =
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val tservice = new TypeManagementServiceInterpreter[IO](repository)
        val iservice = new InstanceManagementServiceInterpreter[IO](tservice)
        (1 to 1).toList.traverse(i =>
          println(i)
          iservice.create(
            "DataCollectionType",
            (
              "id" -> s"ID-$i",
              "version" -> "1.0",
              "organization" -> "HR",
              "sub-organization" -> "Any",
              "domain" -> "Registration",
              "sub-domain" -> "Person",
              "additional-info" -> (
                "info1" -> "ciccio1",
                "info2" -> "ciccio2"
              )
            )
          )
        )
      }

    val cacheRef: IO[Ref[IO, Map[String, EntityType]]] =
      Ref[IO].of(Map.empty[String, EntityType])
    (for {
      cache <- cacheRef
      given Ref[IO, Map[String, EntityType]] = cache
      _ <- loadOntology
      _ <- createEntityType
      res <- instancesCreation
    } yield println(res)).as(ExitCode.Success)
  end run
end TestMain1
