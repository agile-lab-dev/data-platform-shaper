package it.agilelab.dataplatformshaper.domain

import cats.Functor
import cats.data.EitherT
import cats.effect.IO
import io.chrisdavenport.mules.caffeine.CaffeineCache
import io.chrisdavenport.mules.{Cache, TimeSpec}
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.dataplatformshaper.domain.model.l0
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.*
import scala.language.{dynamics, implicitConversions}

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.var"
  )
)
class DomainSpec extends CommonSpec:
  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  given cache: Cache[IO, String, EntityType] = CaffeineCache
    .build[IO, String, EntityType](
      Some(TimeSpec.unsafeFromDuration(1800.second)),
      None,
      None
    )
    .unsafeRunSync()

  var initialStatements: Int = 0

  private val firstType = EntityType(
    "FirstType",
    Set(),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val secondType = EntityType(
    "SecondType",
    Set(),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  def countStatements(
      logger: Logger[IO],
      repository: KnowledgeGraph[IO]
  ): IO[Either[ManagementServiceError, Int]] =
    val query =
      s"""
         |SELECT (COUNT(DISTINCT *) as ?count) WHERE {
         |        ?o ?p ?t
         |}
         |""".stripMargin
    val res = repository.evaluateQuery(query)
    summon[Functor[IO]].map(res)(res =>
      val count = res.toList.headOption
        .flatMap(row => Option(row.getValue("count")))
        .flatMap(_.stringValue().toIntOption)
        .getOrElse(0)
      Right[ManagementServiceError, Int](count)
    )
  end countStatements

  "Counting initial statements" - {
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

        val resultIO = for {
          count <- EitherT(countStatements(logger, repository))
        } yield count

        resultIO.value.flatMap {
          case Right(count) =>
            initialStatements = count
            IO(assert(count >= 0))
          case Left(error) =>
            IO(fail(s"Failed to count statements: ${error.getMessage}"))
        }
      }
    }
  }

  "Creating various types of statements" - {
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
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        (for {
          res1 <- EitherT(tservice.create(firstType))
          res2 <- EitherT(tservice.create(secondType))
          res3 <- EitherT(
            iservice.create(
              "FirstType",
              (
                "field1" -> "value1",
                "field2" -> "value2"
              )
            )
          )
        } yield (res1, res2, res3)).value
      } asserting (ret =>
        ret should matchPattern { case Right(((), (), _)) =>
        }
      )
    }
  }

  "Deleting various types of statements" - {
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
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        (for {
          instances <- EitherT(iservice.list(firstType.name, "", false, None))
          stringInstances = instances.collect { case s: String => s }
          firstInstance = stringInstances.head
          res1 <- EitherT(iservice.delete(firstInstance))
          res2 <- EitherT(tservice.delete(firstType.name))
          res3 <- EitherT(tservice.delete(secondType.name))
        } yield (res1, res2, res3)).value
      } asserting { ret =>
        ret should matchPattern { case Right(((), (), ())) =>
        }
      }
    }
  }


  "Counting final statements" - {
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

        val resultIO = for {
          count <- EitherT(countStatements(logger, repository))
        } yield count

        resultIO.value.flatMap {
          case Right(count) =>
            IO(assert(count.equals(initialStatements)))
          case Left(error) =>
            IO(fail(s"Failed to count statements: ${error.getMessage}"))
        }
      }
    }
  }
end DomainSpec
