package it.agilelab.dataplatformshaper.domain

import cats.data.EitherT
import cats.effect.IO
import io.chrisdavenport.mules.caffeine.CaffeineCache
import io.chrisdavenport.mules.{Cache, TimeSpec}
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.dataplatformshaper.domain.model.l0
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.model.mapping.{
  MappingDefinition,
  MappingKey
}
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  MappingManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.*
import scala.language.{dynamics, implicitConversions}

class MappingSpec extends CommonSpec:

  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  given cache: Cache[IO, String, EntityType] = CaffeineCache
    .build[IO, String, EntityType](
      Some(TimeSpec.unsafeFromDuration(1800.second)),
      None,
      None
    )
    .unsafeRunSync()

  private val sourceType = EntityType(
    "SourceType",
    Set("MappingSource"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val targetType1 = EntityType(
    "TargetType1",
    Set("MappingSource", "MappingTarget"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val targetType2 = EntityType(
    "TargetType2",
    Set("MappingTarget"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val targetType3 = EntityType(
    "TargetType3",
    Set("MappingTarget"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val targetType4 = EntityType(
    "TargetType4",
    Set("MappingTarget"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val mapperTuple = (
    "field1" -> "instance.get('field1')",
    "field2" -> "instance.get('field2')"
  )

  "Creating mapping instances" - {
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
        val repository: Rdf4jKnowledgeGraph[IO] =
          Rdf4jKnowledgeGraph[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        val mservice =
          MappingManagementServiceInterpreter[IO](tservice, iservice)

        (for {
          res1 <- EitherT(tservice.create(sourceType))
          res2 <- EitherT(tservice.create(targetType1))
          res3 <- EitherT(tservice.create(targetType2))
          res4 <- EitherT(tservice.create(targetType3))
          res5 <- EitherT(tservice.create(targetType4))
          res6 <- EitherT(
            mservice.create(
              MappingDefinition(
                MappingKey("mapping1", "SourceType", "TargetType1"),
                mapperTuple
              )
            )
          )
          res7 <- EitherT(
            mservice.create(
              MappingDefinition(
                MappingKey("mapping2", "TargetType1", "TargetType2"),
                mapperTuple
              )
            )
          )
          res8 <- EitherT(
            mservice.create(
              MappingDefinition(
                MappingKey("mapping3", "TargetType1", "TargetType3"),
                mapperTuple
              )
            )
          )
          res9 <- EitherT(
            mservice.create(
              MappingDefinition(
                MappingKey("mapping4", "SourceType", "TargetType4"),
                mapperTuple
              )
            )
          )
          mappers <- EitherT(
            mservice.getMappingsForEntityType(logger, tservice, "SourceType")
          )
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
          mappers
        )).value

      } asserting (ret =>
        ret should matchPattern {
          case Right(((), (), (), (), (), (), (), (), (), _)) =>
        }
      )
    }
  }

  "Automatic creation and update of instances driven by the mappings" - {
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
        val repository: Rdf4jKnowledgeGraph[IO] =
          Rdf4jKnowledgeGraph[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        val mservice =
          MappingManagementServiceInterpreter[IO](tservice, iservice)
        (for {
          res1 <- EitherT(
            iservice.create(
              "SourceType",
              (
                "field1" -> "value1",
                "field2" -> "value2"
              )
            )
          )
          _ <- EitherT(
            mservice.createMappedInstances(res1)
          )
          lt1 <- EitherT(
            iservice.list(
              "TargetType1",
              "field1 = 'value1' and field2 = 'value2'",
              false,
              None
            )
          )
          lt2 <- EitherT(
            iservice.list(
              "TargetType2",
              "field1 = 'value1' and field2 = 'value2'",
              false,
              None
            )
          )
          lt3 <- EitherT(
            iservice.list(
              "TargetType3",
              "field1 = 'value1' and field2 = 'value2'",
              false,
              None
            )
          )
          lt4 <- EitherT(
            iservice.list(
              "TargetType4",
              "field1 = 'value1' and field2 = 'value2'",
              false,
              None
            )
          )
          _ <- EitherT(
            iservice.update(
              res1,
              (
                "field1" -> "value3",
                "field2" -> "value4"
              )
            )
          )
          lt5 <- EitherT(
            iservice.list(
              "TargetType1",
              "field1 = 'value3' and field2 = 'value4'",
              false,
              None
            )
          )
          lt6 <- EitherT(
            iservice.list(
              "TargetType2",
              "field1 = 'value3' and field2 = 'value4'",
              false,
              None
            )
          )
          lt7 <- EitherT(
            iservice.list(
              "TargetType3",
              "field1 = 'value3' and field2 = 'value4'",
              false,
              None
            )
          )
          lt8 <- EitherT(
            iservice.list(
              "TargetType4",
              "field1 = 'value3' and field2 = 'value4'",
              false,
              None
            )
          )
        } yield (
          lt1.length,
          lt2.length,
          lt3.length,
          lt4.length,
          lt5.length,
          lt6.length,
          lt7.length,
          lt8.length
        )).value
      } asserting (ret =>
        ret should matchPattern { case Right((2, 2, 2, 2, 1, 1, 1, 1)) => }
      ) // It's 2 because of the mapper instance
    }
  }

  "Creating a Mapping instance with the same name" - {
    "fails" in {
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
        val repository: Rdf4jKnowledgeGraph[IO] =
          Rdf4jKnowledgeGraph[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        val mservice =
          MappingManagementServiceInterpreter[IO](tservice, iservice)

        (for {
          res <- EitherT(
            mservice.create(
              MappingDefinition(
                MappingKey(
                  "mapping1",
                  "SourceType",
                  "TargetType1"
                ),
                mapperTuple
              )
            )
          )
        } yield res).value

      } asserting (ret => ret should matchPattern { case Left(_) => })
    }
  }

end MappingSpec
