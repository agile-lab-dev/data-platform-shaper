package it.agilelab.dataplatformshaper.domain

import cats.data.EitherT
import cats.effect.IO
import io.chrisdavenport.mules.caffeine.CaffeineCache
import io.chrisdavenport.mules.{Cache, TimeSpec}
import it.agilelab.dataplatformshaper.domain.common.db.Repository
import it.agilelab.dataplatformshaper.domain.common.db.interpreter.Rdf4jSession
import it.agilelab.dataplatformshaper.domain.model.*
import it.agilelab.dataplatformshaper.domain.model.mapping.{
  MappingDefinition,
  MappingKey
}
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.{
  Nullable,
  Repeated
}
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import it.agilelab.dataplatformshaper.domain.service.interpreter.rdf4j.{
  InstanceManagementServiceInterpreter,
  MappingManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import org.scalactic.Equality
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.*
import scala.language.{dynamics, implicitConversions}

@SuppressWarnings(Array("scalafix:DisableSyntax.=="))
class MappingSpec extends CommonSpec:

  given Equality[MappingDefinition] with
    def areEqual(a: MappingDefinition, b: Any): Boolean =
      b match
        case bDef: MappingDefinition =>
          a.mappingKey == bDef.mappingKey &&
          a.mapper.toArray.toSet == bDef.mapper.toArray.toSet &&
          a.additionalSourcesReferences.toSet == bDef.additionalSourcesReferences.toSet
        case _ => false
    end areEqual
  end given

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
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val targetType1 = EntityType(
    "TargetType1",
    Set("MappingSource", "MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val targetType2 = EntityType(
    "TargetType2",
    Set("MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val targetType3 = EntityType(
    "TargetType3",
    Set("MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val targetType4 = EntityType(
    "TargetType4",
    Set("MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val sourceCycleType = EntityType(
    "SourceCycleType",
    Set("MappingSource"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val targetCycleType1 = EntityType(
    "TargetCycleType1",
    Set("MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val targetCycleType2 = EntityType(
    "TargetCycleType2",
    Set("MappingTarget", "MappingSource"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val invalidSourceType = EntityType(
    "InvalidSourceType",
    Set(),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val invalidTargetType = EntityType(
    "InvalidTargetType",
    Set(),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val updateSourceType = EntityType(
    "UpdateSourceType",
    Set("MappingSource"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val updateTargetType = EntityType(
    "UpdateTargetType",
    Set("MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val readSourceType = EntityType(
    "ReadSourceType",
    Set("MappingSource"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val readTargetType = EntityType(
    "ReadTargetType",
    Set("MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val updateMappingSourceType = EntityType(
    "UpdateMappingSourceType",
    Set("MappingSource"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val updateMappingMidType = EntityType(
    "UpdateMappingMidType",
    Set("MappingTarget", "MappingSource"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val updateMappingTargetType = EntityType(
    "UpdateMappingTargetType",
    Set("MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val deleteMappingSourceType = EntityType(
    "DeleteMappingSourceType",
    Set("MappingSource"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val deleteMappingMidType = EntityType(
    "DeleteMappingMidType",
    Set("MappingSource", "MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val deleteMappingTargetType = EntityType(
    "DeleteMappingTargetType",
    Set("MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val twoMappedInstancesSourceType = EntityType(
    "TwoMappedInstancesSourceType",
    Set("MappingSource"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val twoMappedInstancesTargetType = EntityType(
    "TwoMappedInstancesTargetType",
    Set("MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )
  /*
  private val idempotentCreationSourceType = EntityType(
    "IdempotentCreationSourceType",
    Set("MappingSource"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val idempotentCreationMiddleType = EntityType(
    "IdempotentCreationMiddleType",
    Set("MappingSource", "MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val idempotentCreationTargetType = EntityType(
    "IdempotentCreationTargetType",
    Set("MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val idempotentDeletionSourceType = EntityType(
    "IdempotentDeletionSourceType",
    Set("MappingSource"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val idempotentDeletionMiddleType = EntityType(
    "IdempotentDeletionMiddleType",
    Set("MappingSource", "MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val idempotentDeletionTargetType = EntityType(
    "IdempotentDeletionTargetType",
    Set("MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )*/

  private val createDirectlyTargetType = EntityType(
    "CreateDirectlyTargetType",
    Set("MappingTarget"),
    StructType(List("field1" -> StringType(), "field2" -> StringType())): Schema
  )

  private val fileBasedOutputPortType = EntityType(
    "FileBasedOutputPortType",
    Set("MappingSource", "FileBasedOutputPort"),
    StructType(
      List("name" -> StringType(), "additionalParameter" -> StringType())
    ): Schema
  )

  private val tableBasedOutputPortType = EntityType(
    "TableBasedOutputPortType",
    Set("MappingSource", "TableBasedOutputPort"),
    StructType(List("name" -> StringType())): Schema
  )

  private val athenaTableType = EntityType(
    "AthenaTableType",
    Set("MappingTarget"),
    StructType(
      List("name" -> StringType(), "additionalParameter" -> StringType())
    ): Schema
  )

  private val s3Folder = EntityType(
    "S3Folder",
    Set("MappingTarget"),
    StructType(
      List("name" -> StringType(), "additionalParameter" -> StringType())
    ): Schema
  )

  private val nestTargetType = EntityType(
    "NestTargetType",
    Set("MappingTarget"),
    StructType(
      List("age" -> IntType(), "additionalParameter" -> StringType())
    ): Schema
  )

  private val nestSourceType = EntityType(
    "NestSourceType",
    Set("MappingSource", "NestSource"),
    StructType(List("age" -> IntType())): Schema
  )

  private val nestLinkedType = EntityType(
    "NestLinkedType",
    Set("NestLinked"),
    StructType(
      List(
        "name" -> StringType(),
        "parameterToGet" -> StringType(),
        "nestedAttribute" -> StructType(
          List("nest" -> StringType(), "surplusParam" -> StringType())
        )
      )
    ): Schema
  )

  private val readMapSourceType = EntityType(
    "ReadMapSourceType",
    Set("MappingSource", "ReadSource"),
    StructType(List("age" -> IntType())): Schema
  )

  private val readMapTargetType = EntityType(
    "ReadMapTargetType",
    Set("MappingTarget"),
    StructType(
      List(
        "age" -> IntType(),
        "additionalParameter" -> StringType(),
        "secondAdditionalParameter" -> StringType()
      )
    ): Schema
  )

  private val firstReadMapLinkedType = EntityType(
    "FirstReadMapLinkedType",
    Set("FirstReadLinked"),
    StructType(List("name" -> StringType())): Schema
  )

  private val secondReadMapLinkedType = EntityType(
    "SecondReadMapLinkedType",
    Set("SecondReadLinked"),
    StructType(List("surname" -> StringType())): Schema
  )

  private val wrongPathSourceType = EntityType(
    "WrongPathSourceType",
    Set("MappingSource"),
    StructType(List("age" -> IntType())): Schema
  )

  private val wrongPathTargetType = EntityType(
    "WrongPathTargetType",
    Set("MappingTarget"),
    StructType(
      List("age" -> IntType(), "additionalParameter" -> StringType())
    ): Schema
  )

  private val mapperTuple =
    ("field1" -> "source.get('field1')", "field2" -> "source.get('field2')")

  private val updatedMapperTuple =
    ("field1" -> "source.get('field2')", "field2" -> "source.get('field1')")

  "Creating mapping instances" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
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

  "Automatic creation and updates of instances driven by the mappings" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
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
          res1 <- EitherT(
            iservice.create(
              "SourceType",
              ("field1" -> "value5", "field2" -> "value6")
            )
          )
          _ <- EitherT(mservice.createMappedInstances(res1))
          lt1 <- EitherT(
            iservice.list(
              "TargetType1",
              "field1 = 'value5' and field2 = 'value6'",
              false,
              None
            )
          )
          lt2 <- EitherT(
            iservice.list(
              "TargetType2",
              "field1 = 'value5' and field2 = 'value6'",
              false,
              None
            )
          )
          lt3 <- EitherT(
            iservice.list(
              "TargetType3",
              "field1 = 'value5' and field2 = 'value6'",
              false,
              None
            )
          )
          lt4 <- EitherT(
            iservice.list(
              "TargetType4",
              "field1 = 'value5' and field2 = 'value6'",
              false,
              None
            )
          )
          _ <- EitherT(
            iservice.update(res1, ("field1" -> "value7", "field2" -> "value8"))
          )
          _ <- EitherT(mservice.updateMappedInstances(res1))
          lt5 <- EitherT(
            iservice.list(
              "TargetType1",
              "field1 = 'value7' and field2 = 'value8'",
              false,
              None
            )
          )
          lt6 <- EitherT(
            iservice.list(
              "TargetType2",
              "field1 = 'value7' and field2 = 'value8'",
              false,
              None
            )
          )
          lt7 <- EitherT(
            iservice.list(
              "TargetType3",
              "field1 = 'value7' and field2 = 'value8'",
              false,
              None
            )
          )
          lt8 <- EitherT(
            iservice.list(
              "TargetType4",
              "field1 = 'value7' and field2 = 'value8'",
              false,
              None
            )
          )
          res <- EitherT(mservice.readMappedInstances(res1))
        } yield (
          lt1.length,
          lt2.length,
          lt3.length,
          lt4.length,
          lt5.length,
          lt6.length,
          lt7.length,
          lt8.length,
          res
        )).value
      } asserting (ret =>
        ret.map(_(8)).map(_.mkString("\n")).foreach(println)
        ret should matchPattern { case Right((1, 1, 1, 1, 1, 1, 1, 1, _)) => }
      )
    }
  }

  "Trying to read, update, delete a mapping target" - {
    "fails" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val s1 = session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        val mservice =
          MappingManagementServiceInterpreter[IO](tservice, iservice)
        (for {
          res1 <- EitherT(
            iservice.list(
              "TargetType1",
              "field1 = 'value7' and field2 = 'value8'",
              false,
              None
            )
          )
          _ <- EitherT(mservice.readMappedInstances(res1.head match {
            case v: String => v;
            case _         => ""
          }))
        } yield ()).value
      } asserting (_ should matchPattern {
        case Left(ManagementServiceError(List(error)))
            if error.contains(
              "This instance is also a MappingTarget, it's probably not a root in the mapping DAG"
            ) =>
      })

      val s2 = session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        val mservice =
          MappingManagementServiceInterpreter[IO](tservice, iservice)
        (for {
          res1 <- EitherT(
            iservice.list(
              "TargetType1",
              "field1 = 'value7' and field2 = 'value8'",
              false,
              None
            )
          )
          _ <- EitherT(mservice.updateMappedInstances(res1.head match {
            case v: String => v;
            case _         => ""
          }))
        } yield ()).value
      } asserting (_ should matchPattern {
        case Left(ManagementServiceError(List(error)))
            if error.contains(
              "This instance is also a MappingTarget, it's probably not a root in the mapping DAG"
            ) =>
      })

      val s3 = session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val trservice = TraitManagementServiceInterpreter[IO](repository)
        val tservice = TypeManagementServiceInterpreter[IO](trservice)
        val iservice = InstanceManagementServiceInterpreter[IO](tservice)
        val mservice =
          MappingManagementServiceInterpreter[IO](tservice, iservice)
        (for {
          res1 <- EitherT(
            iservice.list(
              "TargetType1",
              "field1 = 'value7' and field2 = 'value8'",
              false,
              None
            )
          )
          _ <- EitherT(mservice.deleteMappedInstances(res1.head match {
            case v: String => v;
            case _         => ""
          }))
        } yield ()).value
      } asserting (_ should matchPattern {
        case Left(ManagementServiceError(List(error)))
            if error.contains(
              "This instance is also a MappingTarget, it's probably not a root in the mapping DAG"
            ) =>
      })

      s1 *> s2 *> s3
    }
  }

  "Automatic deletion of instances driven by the mappings" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
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
          res1 <- EitherT(
            iservice.list(
              "SourceType",
              "field1 = 'value7' and field2 = 'value8'",
              false,
              None
            )
          )
          _ <- EitherT(mservice.deleteMappedInstances(res1.head match {
            case v: String => v; case _ => ""
          }))
          lt1 <- EitherT(
            iservice.list(
              "TargetType1",
              "field1 = 'value7' and field2 = 'value8'",
              false,
              None
            )
          )
          lt2 <- EitherT(
            iservice.list(
              "TargetType2",
              "field1 = 'value7' and field2 = 'value8'",
              false,
              None
            )
          )
          lt3 <- EitherT(
            iservice.list(
              "TargetType3",
              "field1 = 'value7' and field2 = 'value8'",
              false,
              None
            )
          )
          lt4 <- EitherT(
            iservice.list(
              "TargetType4",
              "field1 = 'value7' and field2 = 'value8'",
              false,
              None
            )
          )
        } yield (lt1.length, lt2.length, lt3.length, lt4.length)).value
      } asserting (ret =>
        ret should matchPattern { case Right((0, 0, 0, 0)) => }
      )
    }
  }

  "Creating a Mapping instance with the same name" - {
    "fails" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
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
          res <- EitherT(
            mservice.create(
              MappingDefinition(
                MappingKey("mapping1", "SourceType", "TargetType1"),
                mapperTuple
              )
            )
          )
        } yield res).value

      } asserting (ret =>
        ret should matchPattern {
          case Left(ManagementServiceError(List(error)))
              if error.contains("already exists") =>
        }
      )
    }
  }

  "Creating a mapping cycle" - {
    "fails" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
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
          _ <- EitherT(tservice.create(sourceCycleType))
          _ <- EitherT(tservice.create(targetCycleType1))
          _ <- EitherT(tservice.create(targetCycleType2))
          res1 <- EitherT(
            mservice.create(
              MappingDefinition(
                MappingKey(
                  "cycle_mapping1",
                  "SourceCycleType",
                  "TargetCycleType1"
                ),
                mapperTuple
              )
            )
          )
          res2 <- EitherT(
            mservice.create(
              MappingDefinition(
                MappingKey(
                  "cycle_mapping2",
                  "TargetCycleType2",
                  "TargetCycleType1"
                ),
                mapperTuple
              )
            )
          )
          _ <- EitherT(
            mservice.create(
              MappingDefinition(
                MappingKey(
                  "cycle_mapping3",
                  "TargetCycleType1",
                  "SourceCycleType"
                ),
                mapperTuple
              )
            )
          )
          mappers <- EitherT(
            mservice
              .getMappingsForEntityType(logger, tservice, "SourceCycleType")
          )
        } yield (res1, res2, mappers)).value

      } asserting (ret =>
        ret should matchPattern {
          case Left(ManagementServiceError(List(error)))
              if error.contains(
                "Cycle detected in the hierarchy when processing one of the roots of"
              ) =>
        }
      )
    }
  }

  "Creating a mapping between two EntityTypes which do not have the proper traits" - {
    "fails" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
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
          _ <- EitherT(tservice.create(invalidSourceType))
          _ <- EitherT(tservice.create(invalidTargetType))
          res1 <- EitherT(
            mservice.create(
              MappingDefinition(
                MappingKey(
                  "invalid_mapping1",
                  "InvalidSourceType",
                  "InvalidTargetType"
                ),
                mapperTuple
              )
            )
          )
          mappers <- EitherT(
            mservice
              .getMappingsForEntityType(logger, tservice, "InvalidSourceType")
          )
        } yield (res1, mappers)).value

      } asserting (ret =>
        ret should matchPattern {
          case Left(ManagementServiceError(List(error)))
              if error.contains("does not contain the trait MappingSource") =>
        }
      )
    }
  }

  "Trying to update a mapping target" - {
    "fails" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
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
          _ <- EitherT(tservice.create(updateSourceType))
          _ <- EitherT(tservice.create(updateTargetType))
          _ <- EitherT(
            mservice.create(
              MappingDefinition(
                MappingKey(
                  "update_mapping",
                  "UpdateSourceType",
                  "UpdateTargetType"
                ),
                mapperTuple
              )
            )
          )
          res1 <- EitherT(
            iservice.create(
              "UpdateSourceType",
              ("field1" -> "value10", "field2" -> "value12")
            )
          )
          _ <- EitherT(mservice.createMappedInstances(res1))
          lt1 <- EitherT(
            iservice.list(
              "UpdateTargetType",
              "field1 = 'value10' and field2 = 'value12'",
              false,
              None
            )
          )
          firstElement <- EitherT.fromOption[IO](
            lt1.headOption.collect { case s: String => s },
            "No elements in lt1 or the first element is not a String"
          )
          res <- EitherT(
            iservice.update(
              firstElement,
              ("field1" -> "value13", "field2" -> "value14")
            )
          )
        } yield res).value
      } asserting { ret =>
        ret should matchPattern {
          case Left(ManagementServiceError(List(error)))
              if error.contains("is also a MappingTarget") =>
        }
      }
    }
  }

  "Creating an instance for an EntityType which is a MappingTarget" - {
    "fails" in {
      val session = Rdf4jSession[IO](
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
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)

          (for {
            _ <- EitherT(tservice.create(createDirectlyTargetType))
            res <- EitherT(
              iservice.create(
                createDirectlyTargetType.name,
                ("field1" -> "value10", "field2" -> "value12")
              )
            )
          } yield res).value
        }
        .asserting {
          case Left(error) => succeed
          case _ =>
            fail(s"Expected an error during the creation of the instance")
        }
    }
  }

  "Reading a mapping" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val mappingName = "read_mapping"
      val mappingSourceName = "ReadSourceType"
      val mappingTargetName = "ReadTargetType"
      val expectedMappingKey =
        MappingKey(mappingName, mappingSourceName, mappingTargetName)
      val expectedMappingDef =
        MappingDefinition(expectedMappingKey, mapperTuple)

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)

          (for {
            _ <- EitherT(tservice.create(readSourceType))
            _ <- EitherT(tservice.create(readTargetType))
            _ <- EitherT(mservice.create(expectedMappingDef))
            res <- EitherT(mservice.read(expectedMappingKey))
          } yield res).value
        }
        .asserting {
          case Right(actualMappingDef) =>
            actualMappingDef shouldEqual expectedMappingDef
          case Left(error) =>
            fail(
              s"Expected a successful mapping definition but received error: $error"
            )
        }
    }
  }

  "Updating a mapping" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val secondMappingKey = MappingKey(
        "mapping_mid_target",
        "UpdateMappingMidType",
        "UpdateMappingTargetType"
      )

      val expectedMappingDef =
        MappingDefinition(secondMappingKey, updatedMapperTuple)
      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)

          (for {
            _ <- EitherT(tservice.create(updateMappingSourceType))
            _ <- EitherT(tservice.create(updateMappingMidType))
            _ <- EitherT(tservice.create(updateMappingTargetType))
            firstMappingKey = MappingKey(
              "mapping_source_mid",
              "UpdateMappingSourceType",
              "UpdateMappingMidType"
            )
            _ <- EitherT(
              mservice.create(MappingDefinition(firstMappingKey, mapperTuple))
            )
            _ <- EitherT(
              mservice.create(MappingDefinition(secondMappingKey, mapperTuple))
            )
            sourceId <- EitherT(
              iservice.create(
                "UpdateMappingSourceType",
                Tuple2(("field1", "Hello"), ("field2", "Test"))
              )
            )
            _ <- EitherT(mservice.createMappedInstances(sourceId))
            _ <- EitherT(mservice.update(secondMappingKey, updatedMapperTuple))
            res <- EitherT(mservice.read(secondMappingKey))
          } yield res).value
        }
        .asserting {
          case Right(actualMappingDef) =>
            actualMappingDef shouldEqual expectedMappingDef
          case Left(error) =>
            fail(s"Expected a mapping definition but received error: $error")
        }
    }
  }

  "Deleting a mapping" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val firstMappingKey = MappingKey(
        "delete_mapping_source_mid",
        "DeleteMappingSourceType",
        "DeleteMappingMidType"
      )

      val secondMappingKey = MappingKey(
        "delete_mapping_mid_target",
        "DeleteMappingMidType",
        "DeleteMappingTargetType"
      )

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)

          (for {
            _ <- EitherT(tservice.create(deleteMappingSourceType))
            _ <- EitherT(tservice.create(deleteMappingMidType))
            _ <- EitherT(tservice.create(deleteMappingTargetType))
            _ <- EitherT(
              mservice.create(MappingDefinition(firstMappingKey, mapperTuple))
            )
            _ <- EitherT(
              mservice.create(MappingDefinition(secondMappingKey, mapperTuple))
            )
            _ <- EitherT(
              iservice.create(
                "DeleteMappingSourceType",
                Tuple2(("field1", "Test"), ("field2", "Delete"))
              )
            )
            _ <- EitherT(mservice.delete(firstMappingKey))
            res <- EitherT(mservice.read(secondMappingKey))
          } yield res).value
        }
        .asserting {
          case Left(ManagementServiceError(_)) => succeed
          case _ =>
            fail(
              "Expected a MappingNotFoundError but received a different error or result"
            )
        }
    }
  }

  "Creating the mapped instances more than once" - {
    "fails" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val mappingKey = MappingKey(
        "two_mapping_instances_map",
        "TwoMappedInstancesSourceType",
        "TwoMappedInstancesTargetType"
      )

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)
          (for {
            _ <- EitherT(tservice.create(twoMappedInstancesSourceType))
            _ <- EitherT(tservice.create(twoMappedInstancesTargetType))
            _ <- EitherT(
              mservice.create(MappingDefinition(mappingKey, mapperTuple))
            )
            sourceId <- EitherT(
              iservice.create(
                "TwoMappedInstancesSourceType",
                Tuple2(("field1", "Test"), ("field2", "Double Create"))
              )
            )
            _ <- EitherT(mservice.createMappedInstances(sourceId))
            res <- EitherT(mservice.createMappedInstances(sourceId))
          } yield res).value
        }
        .asserting {
          case Left(ManagementServiceError(_)) => succeed
          case _ =>
            fail(
              "Expected a MappingNotFoundError but received a different error or result"
            )
        }
    }
  }
  /* TODO: Make this test work for both jdbc and rdf4j
  "Checking if the creation of mapped instances is idempotent" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val firstMappingKey = MappingKey(
        "idempotent_source_mid_mapping",
        "IdempotentCreationSourceType",
        "IdempotentCreationMiddleType"
      )

      val secondMappingKey = MappingKey(
        "idempotent_mid_target_mapping",
        "IdempotentCreationMiddleType",
        "IdempotentCreationTargetType"
      )

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)
          (for {
            _ <- EitherT(tservice.create(idempotentCreationSourceType))
            _ <- EitherT(tservice.create(idempotentCreationMiddleType))
            _ <- EitherT(tservice.create(idempotentCreationTargetType))
            _ <- EitherT(
              mservice.create(MappingDefinition(firstMappingKey, mapperTuple))
            )
            _ <- EitherT(
              mservice.create(MappingDefinition(secondMappingKey, mapperTuple))
            )
            sourceId <- EitherT(
              iservice.create(
                "IdempotentCreationSourceType",
                Tuple2(("field1", "Test"), ("field2", "Idempotent"))
              )
            )
            _ <- EitherT(mservice.createMappedInstances(sourceId))
            mappedInstance <- EitherT(
              iservice.list("IdempotentCreationMiddleType", None, true, None)
            )
            mappedEntity = mappedInstance.collect { case e: Entity =>
              e
            }
            tripleToRemove = triple(
              iri(ns, mappedEntity.head.entityId),
              ISCLASSIFIEDBY,
              iri(ns, mappedEntity.head.entityTypeName)
            )
            secondTripleToRemove = triple(
              iri(ns, mappedEntity.head.entityId),
              RDF.TYPE,
              ENTITY
            )
            statementToRemove = statement(tripleToRemove, L2)
            secondStatementToRemove = statement(secondTripleToRemove, L2)
            statementList = List(statementToRemove, secondStatementToRemove)
            _ <- EitherT.liftF(
              repository
                .removeAndInsertStatements(List.empty, statementList)
            )
            _ <- EitherT(mservice.createMappedInstances(sourceId))
            middleTypeElements <- EitherT(
              iservice
                .list(idempotentDeletionMiddleType.name, None, false, None)
            )
            targetTypeElements <- EitherT(
              iservice.list("IdempotentDeletionMiddleType", None, false, None)
            )
            totalElements =
              middleTypeElements.length + targetTypeElements.length
          } yield totalElements).value
        }
        .asserting {
          case Right(numberOfElements) => numberOfElements shouldEqual 0
          case _ =>
            fail("Expected the creation of mapped instances to be idempotent")
        }
    }
  }
   */
  "Creating a mapping between two EntityTypes with a repeated attribute" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val repeatedAttributeSourceType = EntityType(
        "RepeatedAttributeSourceType",
        Set("MappingSource"),
        StructType(
          List(
            "age" -> IntType(),
            "name" -> StringType(),
            "anotherNumber" -> IntType()
          )
        ): Schema
      )

      val repeatedAttributeTargetType = EntityType(
        "RepeatedAttributeTargetType",
        Set("MappingTarget"),
        StructType(
          List("age" -> IntType(Repeated), "name" -> StringType())
        ): Schema
      )

      val mappingKey = MappingKey(
        "repeated_attribute_map",
        repeatedAttributeSourceType.name,
        repeatedAttributeTargetType.name
      )

      val mappingDefinition =
        MappingDefinition(
          mappingKey,
          (
            "age" -> List("source.get('age')", "source.get('anotherNumber')"),
            "name" -> "source.get('name')"
          ),
          Map()
        )

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)
          (for {
            _ <- EitherT(tservice.create(repeatedAttributeSourceType))
            _ <- EitherT(tservice.create(repeatedAttributeTargetType))
            _ <- EitherT(mservice.create(mappingDefinition))
            sourceId <- EitherT(
              iservice.create(
                "RepeatedAttributeSourceType",
                ("age" -> 19, "name" -> "Thomas", "anotherNumber" -> 23)
              )
            )
            _ <- EitherT(mservice.createMappedInstances(sourceId))
            mappedInstances <- EitherT(mservice.readMappedInstances(sourceId))
            mappedInstance = mappedInstances.head._3._2
          } yield mappedInstance).value
        }
        .asserting {
          case Right(entity) =>
            val contained = entity.values.toArray.toSet.exists {
              case ("age", list: List[_]) if list.toSet.equals(Set(19, 23)) =>
                true
              case _ => false
            }
            if contained then succeed
            else fail("Expected field 'age' to have a list with 23 and 19")
          case Left(ex) =>
            fail("Expected a successful created mapping but found an error")
        }
    }
  }

  "Creating a mapping between two EntityTypes with a nullable attribute" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val nullableAttributeSourceType = EntityType(
        "NullableAttributeSourceType",
        Set("MappingSource"),
        StructType(List("age" -> IntType(), "name" -> StringType())): Schema
      )

      val nullableAttributeTargetType = EntityType(
        "NullableAttributeTargetType",
        Set("MappingTarget"),
        StructType(
          List("age" -> IntType(Nullable), "name" -> StringType())
        ): Schema
      )

      val mappingKey = MappingKey(
        "nullable_attribute_map",
        nullableAttributeSourceType.name,
        nullableAttributeTargetType.name
      )

      val mappingDefinition =
        MappingDefinition(
          mappingKey,
          ("age" -> Some("source.get('age')"), "name" -> "source.get('name')"),
          Map()
        )

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)
          (for {
            _ <- EitherT(tservice.create(nullableAttributeSourceType))
            _ <- EitherT(tservice.create(nullableAttributeTargetType))
            _ <- EitherT(mservice.create(mappingDefinition))
            sourceId <- EitherT(
              iservice.create(
                nullableAttributeSourceType.name,
                ("age" -> 19, "name" -> "Thomas")
              )
            )
            _ <- EitherT(mservice.createMappedInstances(sourceId))
            mappedInstances <- EitherT(mservice.readMappedInstances(sourceId))
            mappedInstance = mappedInstances.head._3._2
          } yield mappedInstance).value
        }
        .asserting {
          case Right(entity) =>
            if entity.values.toArray.contains(("age", Some(19))) then succeed
            else fail("Expected field 'age' to contain Some(19)")
          case Left(ex) =>
            fail("Expected a successful created mapping but found an error")
        }
    }
  }

  "Injecting additional references in mapping" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val tableBasedMappingDefinition = MappingDefinition(
        MappingKey(
          "tableBasedMapping",
          "TableBasedOutputPortType",
          "AthenaTableType"
        ),
        (
          "name" -> "source.get('name')",
          "additionalParameter" -> "fileBasedOutputPort.get('additionalParameter')"
        ),
        Map(
          "fileBasedOutputPort" -> "source/dependsOn/FileBasedOutputPortType.find(\"name = 'test'\")/mappedTo/S3Folder"
        )
      )

      val fileBasedMappingDefinition = MappingDefinition(
        MappingKey("fileBasedMapping", "FileBasedOutputPortType", "S3Folder"),
        (
          "name" -> "source.get('name')",
          "additionalParameter" -> "source.get('additionalParameter')"
        ),
        Map()
      )

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)
          (for {
            _ <- EitherT(trservice.create(Trait("FileBasedOutputPort", None)))
            _ <- EitherT(trservice.create(Trait("TableBasedOutputPort", None)))
            _ <- EitherT(
              trservice.link(
                "TableBasedOutputPort",
                Relationship.dependsOn,
                "FileBasedOutputPort"
              )
            )

            _ <- EitherT(tservice.create(fileBasedOutputPortType))
            _ <- EitherT(tservice.create(tableBasedOutputPortType))
            _ <- EitherT(tservice.create(athenaTableType))
            _ <- EitherT(tservice.create(s3Folder))

            _ <- EitherT(mservice.create(fileBasedMappingDefinition))
            _ <- EitherT(mservice.create(tableBasedMappingDefinition))

            fileBasedSourceId <- EitherT(
              iservice
                .create(
                  "FileBasedOutputPortType",
                  (("name", "test"), ("additionalParameter", "addTest"))
                )
            )
            tableBasedSourceId <- EitherT(
              iservice
                .create("TableBasedOutputPortType", Tuple1("name", "test"))
            )
            _ <- EitherT(
              iservice.link(
                tableBasedSourceId,
                Relationship.dependsOn,
                fileBasedSourceId
              )
            )
            _ <- EitherT(mservice.createMappedInstances(fileBasedSourceId))
            _ <- EitherT(mservice.createMappedInstances(tableBasedSourceId))
            tableMappedInstances <- EitherT(
              mservice.readMappedInstances(tableBasedSourceId)
            )
            targetEntity = tableMappedInstances.head._3._2
            fileMappedInstances <- EitherT(
              mservice.readMappedInstances(fileBasedSourceId)
            )
            expectedEntity = fileMappedInstances.head._3._2
          } yield (targetEntity, expectedEntity)).value
        }
        .asserting {
          case Right((targetEntity, expectedEntity))
              if targetEntity.values.equals(expectedEntity.values) =>
            succeed
          case _ =>
            fail(
              "Expected a successful creation of mapped instances but found an error"
            )
        }
    }
  }

  "Injecting additional source references when there are multiple instances" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val mappingDefinition = MappingDefinition(
        MappingKey(
          "nestedMappingDefinition",
          "NestSourceType",
          "NestTargetType"
        ),
        (
          "age" -> "source.get('age')",
          "additionalParameter" -> "nestLinkedType.get('nestedAttribute/nest')"
        ),
        Map(
          "nestLinkedType" -> "source/hasPart/NestLinkedType.find(\"nestedAttribute/nest = 'testNest'\")"
        )
      )

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)
          (for {
            _ <- EitherT(trservice.create(Trait("NestSource", None)))
            _ <- EitherT(trservice.create(Trait("NestLinked", None)))
            _ <- EitherT(
              trservice.link("NestSource", Relationship.hasPart, "NestLinked")
            )

            _ <- EitherT(tservice.create(nestSourceType))
            _ <- EitherT(tservice.create(nestTargetType))
            _ <- EitherT(tservice.create(nestLinkedType))

            _ <- EitherT(mservice.create(mappingDefinition))

            nestedSourceId <- EitherT(
              iservice
                .create("NestSourceType", Tuple1("age", 23))
            )
            nestLinkedId <- EitherT(
              iservice
                .create(
                  "NestLinkedType",
                  (
                    "name" -> "test",
                    "parameterToGet" -> "getTest",
                    "nestedAttribute" -> (
                      "nest" -> "testNest",
                      "surplusParam" -> "surplusValue"
                    )
                  )
                )
            )
            _ <- EitherT(
              iservice.link(nestedSourceId, Relationship.hasPart, nestLinkedId)
            )
            _ <- EitherT(
              iservice
                .create(
                  "NestLinkedType",
                  (
                    "name" -> "secondTest",
                    "parameterToGet" -> "secondGetTest",
                    "nestedAttribute" -> (
                      "nest" -> "secondTestNest",
                      "surplusParam" -> "secondSurplusValue"
                    )
                  )
                )
            )
            res <- EitherT(mservice.createMappedInstances(nestedSourceId))
            nestSourceInstances <- EitherT(
              mservice.readMappedInstances(nestedSourceId)
            )
            targetEntity = nestSourceInstances.head._3._2
          } yield targetEntity).value
        }
        .asserting {
          case Right(targetEntity) =>
            if targetEntity.values.toArray.toSet
                .contains(("additionalParameter", "testNest"))
            then succeed
            else
              fail(
                "Expected the created type to have 'additionalParameter' with value 'testNest'"
              )
          case _ =>
            fail(
              "Expected a successful creation of mapped instances but found an error"
            )
        }
    }
  }

  "Injecting additional source references using the mapping name in the path" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val mapNameSourceType = EntityType(
        "MapNameSourceType",
        Set("MappingSource", "MapNameSource"),
        StructType(List("age" -> IntType())): Schema
      )

      val mapNameTargetType = EntityType(
        "MapNameTargetType",
        Set("MappingTarget"),
        StructType(
          List("age" -> IntType(), "additionalParameter" -> StringType())
        ): Schema
      )

      val otherMapNameSourceType = EntityType(
        "OtherMapNameSourceType",
        Set("MappingSource", "OtherMapNameSource"),
        StructType(
          List("name" -> StringType(), "surname" -> StringType())
        ): Schema
      )

      val otherMapNameTargetType = EntityType(
        "OtherMapNameTargetType",
        Set("MappingTarget"),
        StructType(
          List("name" -> StringType(), "surname" -> StringType())
        ): Schema
      )

      val otherMappingDefinition = MappingDefinition(
        MappingKey(
          "otherMappingNameMappingDefinition",
          otherMapNameSourceType.name,
          otherMapNameTargetType.name
        ),
        ("name" -> "source.get('name')", "surname" -> "source.get('surname')"),
        Map()
      )

      val mappingDefinition = MappingDefinition(
        MappingKey(
          "mappingNameMappingDefinition",
          mapNameSourceType.name,
          mapNameTargetType.name
        ),
        (
          "age" -> "source.get('age')",
          "additionalParameter" -> "target.get('name')"
        ),
        Map(
          "target" -> "source/hasPart/OtherMapNameSourceType/mappedTo#otherMappingNameMappingDefinition/OtherMapNameTargetType"
        )
      )

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)
          (for {
            _ <- EitherT(trservice.create(Trait("MapNameSource", None)))
            _ <- EitherT(trservice.create(Trait("OtherMapNameSource", None)))
            _ <- EitherT(
              trservice.link(
                "MapNameSource",
                Relationship.hasPart,
                "OtherMapNameSource"
              )
            )

            _ <- EitherT(tservice.create(mapNameSourceType))
            _ <- EitherT(tservice.create(mapNameTargetType))
            _ <- EitherT(tservice.create(otherMapNameSourceType))
            _ <- EitherT(tservice.create(otherMapNameTargetType))

            _ <- EitherT(mservice.create(otherMappingDefinition))
            _ <- EitherT(mservice.create(mappingDefinition))

            mapNameSourceId <- EitherT(
              iservice
                .create(mapNameSourceType.name, Tuple1("age", 23))
            )
            otherMapNameSourceId <- EitherT(
              iservice.create(
                otherMapNameSourceType.name,
                ("name" -> "John", "surname" -> "Marston")
              )
            )
            _ <- EitherT(
              iservice.link(
                mapNameSourceId,
                Relationship.hasPart,
                otherMapNameSourceId
              )
            )
            _ <- EitherT(mservice.createMappedInstances(otherMapNameSourceId))
            _ <- EitherT(mservice.createMappedInstances(mapNameSourceId))
            mappedInstances <- EitherT(
              mservice.readMappedInstances(mapNameSourceId)
            )
            res = mappedInstances.head._3._2
          } yield res).value
        }
        .asserting {
          case Right(entity) =>
            if entity.values.toArray.contains(("additionalParameter", "John"))
            then succeed
            else fail("Expected additionalParameter with value 'John'")
          case _ =>
            fail(
              "Expected a successful creation of mapped instances but found an error"
            )
        }
    }
  }

  "Injecting additional source references using the partOf relationship" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val isPartSourceType = EntityType(
        "IsPartSourceType",
        Set("MappingSource", "IsPartSource"),
        StructType(List("age" -> IntType())): Schema
      )

      val isPartTargetType = EntityType(
        "IsPartTargetType",
        Set("MappingTarget"),
        StructType(
          List("age" -> IntType(), "additionalParameter" -> StringType())
        ): Schema
      )

      val isPartLinkedType = EntityType(
        "IsPartLinkedType",
        Set("IsPartLinked"),
        StructType(List("name" -> StringType())): Schema
      )

      val mappingDefinition = MappingDefinition(
        MappingKey(
          "partOfMappingDefinition",
          isPartSourceType.name,
          isPartTargetType.name
        ),
        (
          "age" -> "source.get('age')",
          "additionalParameter" -> "target.get('name')"
        ),
        Map("target" -> "source/partOf/IsPartLinkedType")
      )

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)
          (for {
            _ <- EitherT(trservice.create(Trait("IsPartSource", None)))
            _ <- EitherT(trservice.create(Trait("IsPartLinked", None)))
            _ <- EitherT(
              trservice
                .link("IsPartLinked", Relationship.hasPart, "IsPartSource")
            )

            _ <- EitherT(tservice.create(isPartSourceType))
            _ <- EitherT(tservice.create(isPartTargetType))
            _ <- EitherT(tservice.create(isPartLinkedType))

            _ <- EitherT(mservice.create(mappingDefinition))

            isPartSourceId <- EitherT(
              iservice
                .create(isPartSourceType.name, Tuple1("age", 33))
            )
            hasPartsSourceId <- EitherT(
              iservice.create(isPartLinkedType.name, Tuple1("name", "Jim"))
            )
            _ <- EitherT(
              iservice
                .link(hasPartsSourceId, Relationship.hasPart, isPartSourceId)
            )
            _ <- EitherT(mservice.createMappedInstances(isPartSourceId))
            mappedInstances <- EitherT(
              mservice.readMappedInstances(isPartSourceId)
            )
            res = mappedInstances.head._3._2
          } yield res).value
        }
        .asserting {
          case Right(entity) =>
            if entity.values.toArray.contains(("additionalParameter", "Jim"))
            then succeed
            else fail("Expected additionalParameter with value 'Jim'")
          case _ =>
            fail(
              "Expected a successful creation of mapped instances but found an error"
            )
        }
    }
  }

  "Updating mapped instances with additional references" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val updateMapSourceType = EntityType(
        "UpdateMapSourceType",
        Set("MappingSource", "UpdateSource"),
        StructType(List("age" -> IntType())): Schema
      )

      val updateMapTargetType = EntityType(
        "UpdateMapTargetType",
        Set("MappingTarget"),
        StructType(
          List("age" -> IntType(), "additionalParameter" -> IntType())
        ): Schema
      )

      val updateMapLinkedType = EntityType(
        "UpdateMapLinkedType",
        Set("UpdateLinked"),
        StructType(
          List("age" -> IntType(), "additionalParameter" -> IntType())
        ): Schema
      )

      val mappingDefinition = MappingDefinition(
        MappingKey(
          "updateLinkedMappingDefinition",
          "UpdateMapSourceType",
          "UpdateMapTargetType"
        ),
        (
          "age" -> "source.get('age')",
          "additionalParameter" -> "updateLinkedType.get('additionalParameter')"
        ),
        Map("updateLinkedType" -> "source/dependsOn/UpdateMapLinkedType")
      )

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)
          (for {
            _ <- EitherT(trservice.create(Trait("UpdateSource", None)))
            _ <- EitherT(trservice.create(Trait("UpdateLinked", None)))
            _ <- EitherT(
              trservice
                .link("UpdateSource", Relationship.dependsOn, "UpdateLinked")
            )

            _ <- EitherT(tservice.create(updateMapSourceType))
            _ <- EitherT(tservice.create(updateMapTargetType))
            _ <- EitherT(tservice.create(updateMapLinkedType))

            _ <- EitherT(mservice.create(mappingDefinition))

            updateSourceId <- EitherT(
              iservice
                .create(updateMapSourceType.name, Tuple1("age", 23))
            )
            updateLinkedId <- EitherT(
              iservice
                .create(
                  updateMapLinkedType.name,
                  ("age" -> 23, "additionalParameter" -> 99)
                )
            )
            _ <- EitherT(
              iservice
                .link(updateSourceId, Relationship.dependsOn, updateLinkedId)
            )
            _ <- EitherT(mservice.createMappedInstances(updateSourceId))
            _ <- EitherT(
              iservice.update(
                updateLinkedId,
                ("age" -> 23, "additionalParameter" -> 1)
              )
            )
            _ <- EitherT(mservice.updateMappedInstances(updateSourceId))
            updateSourceInstances <- EitherT(
              mservice.readMappedInstances(updateSourceId)
            )
            targetEntity = updateSourceInstances.head._3._2
          } yield targetEntity).value
        }
        .asserting {
          case Right(targetEntity) =>
            if targetEntity.values.toArray.toSet
                .contains(("additionalParameter", 1))
            then succeed
            else
              fail(
                "Expected the created type to have 'additionalParameter' with value 1"
              )
          case _ =>
            fail(
              "Expected a successful creation of mapped instances but found an error"
            )
        }
    }
  }

  "Reading a mapping definition after injecting additional references" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val mappingDefinition = MappingDefinition(
        MappingKey(
          "readMappingDefinition",
          "ReadMapSourceType",
          "ReadMapTargetType"
        ),
        (
          "age" -> "source.get('age')",
          "additionalParameter" -> "readLinkedType.get('name')",
          "secondAdditionalParameter" -> "secondReadLinkedType.get('surname')"
        ),
        Map(
          "readLinkedType" -> "source/dependsOn/FirstReadMapLinkedType",
          "secondReadLinkedType" -> "source/hasPart/SecondReadMapLinkedType"
        )
      )

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)
          (for {
            _ <- EitherT(trservice.create(Trait("ReadSource", None)))
            _ <- EitherT(trservice.create(Trait("FirstReadLinked", None)))
            _ <- EitherT(trservice.create(Trait("SecondReadLinked", None)))
            _ <- EitherT(
              trservice
                .link("ReadSource", Relationship.dependsOn, "FirstReadLinked")
            )
            _ <- EitherT(
              trservice
                .link("ReadSource", Relationship.hasPart, "SecondReadLinked")
            )

            _ <- EitherT(tservice.create(readMapSourceType))
            _ <- EitherT(tservice.create(readMapTargetType))
            _ <- EitherT(tservice.create(firstReadMapLinkedType))
            _ <- EitherT(tservice.create(secondReadMapLinkedType))

            _ <- EitherT(mservice.create(mappingDefinition))
            res <- EitherT(mservice.read(mappingDefinition.mappingKey))
          } yield res).value
        }
        .asserting {
          case Right(m) => m shouldEqual mappingDefinition
          case _ =>
            fail(
              "The MappingDefinition read is not equal to the starting MappingDefinition"
            )
        }
    }
  }

  "Injecting additional source references with a wrong path" - {
    "fails" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val mappingDefinition = MappingDefinition(
        MappingKey(
          "wrongMappingDefinition",
          "WrongPathSourceType",
          "WrongPathTargetType"
        ),
        (
          "age" -> "source.get('age')",
          "additionalParameter" -> "wrongType.get('nestedAttribute/nest')"
        ),
        Map("wrongType" -> "source/WrongType")
      )

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)
          (for {
            _ <- EitherT(tservice.create(wrongPathSourceType))
            _ <- EitherT(tservice.create(wrongPathTargetType))
            _ <- EitherT(mservice.create(mappingDefinition))

            nestedSourceId <- EitherT(
              iservice
                .create("WrongPathSourceType", Tuple1("age", 23))
            )
            res <- EitherT(mservice.createMappedInstances(nestedSourceId))
          } yield res).value
        }
        .asserting {
          case Left(_) => succeed
          case _ =>
            fail("Expected an error during the creation of the mapping")
        }
    }
  }
/* TODO: Make this test work for both jdbc and rdf4j
  "Checking if the deletion of mapped instances is idempotent" - {
    "works" in {
      val session = Rdf4jSession[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val firstMappingKey = MappingKey(
        "idempotent_delete_source_mid_mapping",
        "IdempotentDeletionSourceType",
        "IdempotentDeletionMiddleType"
      )

      val secondMappingKey = MappingKey(
        "idempotent_delete_mid_target_mapping",
        "IdempotentDeletionMiddleType",
        "IdempotentDeletionTargetType"
      )

      session
        .use { session =>
          val repository: Repository[IO] = getRepository[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)
          val mservice =
            MappingManagementServiceInterpreter[IO](tservice, iservice)
          (for {
            _ <- EitherT(tservice.create(idempotentDeletionSourceType))
            _ <- EitherT(tservice.create(idempotentDeletionMiddleType))
            _ <- EitherT(tservice.create(idempotentDeletionTargetType))
            _ <- EitherT(
              mservice.create(MappingDefinition(firstMappingKey, mapperTuple))
            )
            _ <- EitherT(
              mservice.create(MappingDefinition(secondMappingKey, mapperTuple))
            )
            sourceId <- EitherT(
              iservice.create(
                "IdempotentDeletionSourceType",
                Tuple2(("field1", "Test"), ("field2", "Idempotent"))
              )
            )
            _ <- EitherT(mservice.createMappedInstances(sourceId))
            firstMappedInstance <- EitherT(
              iservice.list("IdempotentDeletionMiddleType", None, true, None)
            )
            firstMappedEntity = firstMappedInstance.collect { case e: Entity =>
              e
            }
            secondMappedInstance <- EitherT(
              iservice.list("IdempotentDeletionTargetType", None, true, None)
            )
            secondMappedEntity = secondMappedInstance.collect {
              case e: Entity =>
                e
            }
            tripleToRemove = triple(
              iri(ns, sourceId),
              iri(ns, s"mappedTo#${firstMappingKey.mappingName}"),
              iri(ns, firstMappedEntity.head.entityId)
            )
            secondTripleToRemove = triple(
              iri(ns, firstMappedEntity.head.entityId),
              iri(ns, s"mappedTo#${secondMappingKey.mappingName}"),
              iri(ns, secondMappedEntity.head.entityId)
            )
            statementToRemove = statement(tripleToRemove, L2)
            secondStatementToRemove = statement(secondTripleToRemove, L2)
            _ <- EitherT.liftF(
              repository
                .removeAndInsertStatements(
                  List.empty,
                  List(statementToRemove, secondStatementToRemove)
                )
            )
            _ <- EitherT(mservice.deleteMappedInstances(sourceId))
            middleTypeElements <- EitherT(
              iservice
                .list(idempotentDeletionMiddleType.name, None, false, None)
            )
            targetTypeElements <- EitherT(
              iservice
                .list(idempotentDeletionTargetType.name, None, false, None)
            )
            totalElements =
              middleTypeElements.length + targetTypeElements.length
          } yield totalElements).value
        }
        .asserting {
          case Right(numberOfElements) => numberOfElements shouldEqual 0
          case _ =>
            fail("Expected the creation of mapped instances to be idempotent")
        }
    }
  }
 */

end MappingSpec
