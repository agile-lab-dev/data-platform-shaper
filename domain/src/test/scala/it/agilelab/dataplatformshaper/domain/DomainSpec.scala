package it.agilelab.dataplatformshaper.domain

import cats.data.EitherT
import cats.effect.IO
import io.chrisdavenport.mules.caffeine.CaffeineCache
import io.chrisdavenport.mules.{Cache, TimeSpec}
import it.agilelab.dataplatformshaper.domain.common.db.Repository
import it.agilelab.dataplatformshaper.domain.model.*
import it.agilelab.dataplatformshaper.domain.model.Relationship.hasPart
import it.agilelab.dataplatformshaper.domain.model.mapping.{
  MappingDefinition,
  MappingKey
}
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.interpreter.rdf4j.{
  InstanceManagementServiceInterpreter,
  MappingManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.*
import scala.language.{dynamics, implicitConversions}

@SuppressWarnings(Array("scalafix:DisableSyntax.var"))
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
    Set("MappingSource"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val secondType = EntityType(
    "SecondType",
    Set("Trait1"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val thirdType = EntityType(
    "ThirdType",
    Set("MappingTarget", "MappingSource"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val fourthType = EntityType(
    "FourthType",
    Set("MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val mapperTuple =
    ("field1" -> "source.get('field1')", "field2" -> "source.get('field2')")

  /* TODO: Make this work for both jdbc and rdf4j
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
   */

  /* TODO: Make this work for both jdbc and rdf4j
  "Counting initial statements" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository(session)

        val resultIO = for {
          count <- EitherT(countStatements(logger, repository))
        } yield count

        resultIO.value.flatMap {
          case Right(count) =>
            initialStatements = count
            IO(assert(count >= 0))
          case Left(error) =>
            IO(fail(s"Failed to count statements: ${error}"))
        }
      }
    }
  }
   */

  "Creating various types of statements" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        val mservice =
          MappingManagementServiceInterpreter[IO](tservice, iservice)
        (for {
          _ <- EitherT(trservice.create(Trait("Trait1", None)))
          _ <- EitherT(trservice.create(Trait("Trait2", Some("Trait1"))))
          _ <- EitherT(trservice.link("Trait1", hasPart, "Trait2"))
          res1 <- EitherT(tservice.create(firstType))
          res2 <- EitherT(tservice.create(secondType))
          res3 <- EitherT(tservice.create(thirdType))
          res4 <- EitherT(tservice.create(fourthType))
          res5 <- EitherT(
            iservice
              .create("FirstType", ("field1" -> "value1", "field2" -> "value2"))
          )
          res6 <- EitherT(
            iservice.create(
              "SecondType",
              ("field1" -> "value3", "field2" -> "value4")
            )
          )
          res7 <- EitherT(
            mservice.create(
              MappingDefinition(
                MappingKey("mapping1", "FirstType", "ThirdType"),
                mapperTuple
              )
            )
          )
          res8 <- EitherT(
            mservice.create(
              MappingDefinition(
                MappingKey("mapping2", "ThirdType", "FourthType"),
                mapperTuple
              )
            )
          )
          _ <- EitherT(mservice.createMappedInstances(res5))
        } yield (res1, res2, res3, res4, res5, res6, res7, res8)).value
      } asserting (ret =>
        ret should matchPattern { case Right((_, _, _, _, _, _, _, _)) =>
        }
      )
    }
  }

  "Deleting various types of statements" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        val mservice =
          MappingManagementServiceInterpreter[IO](tservice, iservice)
        (for {
          firstInstances <- EitherT(
            iservice.list(firstType.name, "", false, None)
          )
          firstStringInstances = firstInstances.collect { case s: String => s }
          firstStringInstance = firstStringInstances.head
          secondInstances <- EitherT(
            iservice.list(secondType.name, "", false, None)
          )
          secondStringInstances = secondInstances.collect { case s: String =>
            s
          }
          secondStringInstance = secondStringInstances.head
          _ <- EitherT(mservice.deleteMappedInstances(firstStringInstance))
          res1 <- EitherT(iservice.delete(firstStringInstance))
          res2 <- EitherT(iservice.delete(secondStringInstance))
          res3 <- EitherT(
            mservice.delete(MappingKey("mapping1", "FirstType", "ThirdType"))
          )
          res4 <- EitherT(tservice.delete(firstType.name))
          res5 <- EitherT(tservice.delete(secondType.name))
          res6 <- EitherT(tservice.delete(thirdType.name))
          res7 <- EitherT(tservice.delete(fourthType.name))
          res8 <- EitherT(trservice.unlink("Trait1", hasPart, "Trait2"))
          res9 <- EitherT(trservice.delete("Trait2"))
          res10 <- EitherT(trservice.delete("Trait1"))
        } yield (
          res1,
          res2,
          res3,
          res4,
          res5,
          res6,
          res7,
          res8,
          res9,
          res10
        )).value
      } asserting { ret =>
        ret should matchPattern {
          case Right(((), (), (), (), (), (), (), (), (), ())) =>
        }
      }
    }
  }

  /* TODO: Make this work for both jdbc and rdf4j
  "Counting final statements" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)

        val resultIO = for {
          count <- EitherT(countStatements(logger, repository))
        } yield count

        resultIO.value.flatMap {
          case Right(count) =>
            IO(assert(count.equals(initialStatements)))
          case Left(error) =>
            IO(fail(s"Failed to count statements: ${error}"))
        }
      }
    }
  }
   */
end DomainSpec
