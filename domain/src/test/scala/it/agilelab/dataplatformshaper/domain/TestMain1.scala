package it.agilelab.dataplatformshaper.domain

import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Ref}
import cats.syntax.all.*
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.dataplatformshaper.domain.model.NS.*
import it.agilelab.dataplatformshaper.domain.model.l0
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import org.eclipse.rdf4j.model.util.Values.iri
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import scala.jdk.CollectionConverters.*

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.null"
  )
)
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
        val trms = new TraitManagementServiceInterpreter[IO](repository)
        val service = new TypeManagementServiceInterpreter[IO](trms)
        (for {
          _ <- EitherT[IO, ManagementServiceError, Unit](
            service.create(
              EntityType(
                "CommonType",
                StructType(List("id" -> StringType()))
              )
            )
          )
          father <- EitherT[IO, ManagementServiceError, EntityType](
            service.read("CommonType")
          )
          _ <- EitherT[IO, ManagementServiceError, Unit](
            service.create(
              l0.EntityType(
                "DataCollectionType",
                Set("DataCollection"),
                StructType(
                  List(
                    "organization" -> StringType(),
                    "sub-organization" -> StringType(),
                    "domain" -> StringType(),
                    "sub-domain" -> StringType(),
                    "foundation" -> DateType(),
                    "timestamp" -> TimestampDataType(),
                    "double" -> DoubleType(),
                    "float" -> FloatType(),
                    "long" -> LongType(),
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
        val trms = new TraitManagementServiceInterpreter[IO](repository)
        val tservice = new TypeManagementServiceInterpreter[IO](trms)
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
              "foundation" -> LocalDate.of(2008, 8, 26),
              "timestamp" -> ZonedDateTime
                .of(2023, 10, 11, 12, 0, 0, 0, ZoneId.of("Europe/London")),
              "double" -> 1.23,
              "float" -> 1.23f,
              "long" -> 123L,
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
