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
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  MappingManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import org.scalactic.Equality
import org.eclipse.rdf4j.model.util.Statements.statement
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.*
import it.agilelab.dataplatformshaper.domain.model.NS.{ISCLASSIFIEDBY, ns, L2}
import org.eclipse.rdf4j.model.util.Values.{iri, triple}

import scala.language.{dynamics, implicitConversions}

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.=="
  )
)
class MappingSpec extends CommonSpec:

  given Equality[MappingDefinition] with
    def areEqual(a: MappingDefinition, b: Any): Boolean =
      b match
        case bDef: MappingDefinition =>
          a.mappingKey == bDef.mappingKey &&
          a.mapper.productIterator.toSet == bDef.mapper.productIterator.toSet
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

  private val sourceCycleType = EntityType(
    "SourceCycleType",
    Set("MappingSource"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val targetCycleType1 = EntityType(
    "TargetCycleType1",
    Set("MappingTarget"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val targetCycleType2 = EntityType(
    "TargetCycleType2",
    Set("MappingTarget", "MappingSource"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val invalidSourceType = EntityType(
    "InvalidSourceType",
    Set(),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val invalidTargetType = EntityType(
    "InvalidTargetType",
    Set(),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val updateSourceType = EntityType(
    "UpdateSourceType",
    Set("MappingSource"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val updateTargetType = EntityType(
    "UpdateTargetType",
    Set("MappingTarget"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val readSourceType = EntityType(
    "ReadSourceType",
    Set("MappingSource"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val readTargetType = EntityType(
    "ReadTargetType",
    Set("MappingTarget"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val updateMappingSourceType = EntityType(
    "UpdateMappingSourceType",
    Set("MappingSource"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val updateMappingMidType = EntityType(
    "UpdateMappingMidType",
    Set("MappingTarget", "MappingSource"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val updateMappingTargetType = EntityType(
    "UpdateMappingTargetType",
    Set("MappingTarget"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val deleteMappingSourceType = EntityType(
    "DeleteMappingSourceType",
    Set("MappingSource"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val deleteMappingMidType = EntityType(
    "DeleteMappingMidType",
    Set("MappingSource", "MappingTarget"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val deleteMappingTargetType = EntityType(
    "DeleteMappingTargetType",
    Set("MappingTarget"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val twoMappedInstancesSourceType = EntityType(
    "TwoMappedInstancesSourceType",
    Set("MappingSource"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val twoMappedInstancesTargetType = EntityType(
    "TwoMappedInstancesTargetType",
    Set("MappingTarget"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val idempotentCreationSourceType = EntityType(
    "IdempotentCreationSourceType",
    Set("MappingSource"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val idempotentCreationMiddleType = EntityType(
    "IdempotentCreationMiddleType",
    Set("MappingSource", "MappingTarget"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val idempotentCreationTargetType = EntityType(
    "IdempotentCreationTargetType",
    Set("MappingTarget"),
    StructType(
      List(
        "field1" -> StringType(),
        "field2" -> StringType()
      )
    ): Schema
  )

  private val createDirectlyTargetType = EntityType(
    "CreateDirectlyTargetType",
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

  private val updatedMapperTuple = (
    "field1" -> "instance.get('field2')",
    "field2" -> "instance.get('field1')"
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

  "Automatic creation and updates of instances driven by the mappings" - {
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
                "field1" -> "value5",
                "field2" -> "value6"
              )
            )
          )
          _ <- EitherT(
            mservice.createMappedInstances(res1)
          )
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
            iservice.update(
              res1,
              (
                "field1" -> "value7",
                "field2" -> "value8"
              )
            )
          )
          _ <- EitherT(
            mservice.updateMappedInstances(res1)
          )
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
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )

      val s1 = session.use { session =>
        val repository: Rdf4jKnowledgeGraph[IO] =
          Rdf4jKnowledgeGraph[IO](session)
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
        val repository: Rdf4jKnowledgeGraph[IO] =
          Rdf4jKnowledgeGraph[IO](session)
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
        val repository: Rdf4jKnowledgeGraph[IO] =
          Rdf4jKnowledgeGraph[IO](session)
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
        } yield (
          lt1.length,
          lt2.length,
          lt3.length,
          lt4.length
        )).value
      } asserting (ret =>
        ret should matchPattern { case Right((0, 0, 0, 0)) => }
      )
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
        } yield (
          res1,
          res2,
          mappers
        )).value

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
        } yield (
          res1,
          mappers
        )).value

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
              (
                "field1" -> "value10",
                "field2" -> "value12"
              )
            )
          )
          _ <- EitherT(
            mservice.createMappedInstances(res1)
          )
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
              (
                "field1" -> "value13",
                "field2" -> "value14"
              )
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
      val session = Session[IO](
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
          val repository: Rdf4jKnowledgeGraph[IO] =
            Rdf4jKnowledgeGraph[IO](session)
          val trservice = TraitManagementServiceInterpreter[IO](repository)
          val tservice = TypeManagementServiceInterpreter[IO](trservice)
          val iservice = InstanceManagementServiceInterpreter[IO](tservice)

          (for {
            _ <- EitherT(tservice.create(createDirectlyTargetType))
            res <- EitherT(
              iservice.create(
                createDirectlyTargetType.name,
                (
                  "field1" -> "value10",
                  "field2" -> "value12"
                )
              )
            )
          } yield res).value
        }
        .asserting {
          case Left(error) => succeed
          case _ =>
            fail(
              s"Expected an error during the creation of the instance"
            )
        }
    }
  }

  "Reading a mapping" - {
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
      val mappingName = "read_mapping"
      val mappingSourceName = "ReadSourceType"
      val mappingTargetName = "ReadTargetType"
      val expectedMappingKey =
        MappingKey(mappingName, mappingSourceName, mappingTargetName)
      val expectedMappingDef =
        MappingDefinition(expectedMappingKey, mapperTuple)

      session
        .use { session =>
          val repository: Rdf4jKnowledgeGraph[IO] =
            Rdf4jKnowledgeGraph[IO](session)
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
      val session = Session[IO](
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
          val repository: Rdf4jKnowledgeGraph[IO] =
            Rdf4jKnowledgeGraph[IO](session)
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
            fail(
              s"Expected a mapping definition but received error: $error"
            )
        }
    }
  }

  "Deleting a mapping" - {
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
          val repository: Rdf4jKnowledgeGraph[IO] =
            Rdf4jKnowledgeGraph[IO](session)
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
      val session = Session[IO](
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
          val repository: Rdf4jKnowledgeGraph[IO] =
            Rdf4jKnowledgeGraph[IO](session)
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

  "Checking if the creation of mapped instances is idempotent" - {
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
          val repository: Rdf4jKnowledgeGraph[IO] =
            Rdf4jKnowledgeGraph[IO](session)
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
              iservice.list("IdempotentCreationTargetType", None, true, None)
            )
            mappedEntity = mappedInstance.collect { case e: Entity =>
              e
            }
            tripleToRemove = triple(
              iri(ns, mappedEntity.head.entityId),
              ISCLASSIFIEDBY,
              iri(ns, mappedEntity.head.entityTypeName)
            )
            statementToRemove = statement(tripleToRemove, L2)
            _ <- EitherT.liftF(
              repository
                .removeAndInsertStatements(List.empty, List(statementToRemove))
            )
            res <- EitherT(mservice.createMappedInstances(sourceId))
          } yield res).value
        }
        .asserting {
          case Right(()) => succeed
          case _ =>
            fail("Expected the creation of mapped instances to be idempotent")
        }
    }
  }

end MappingSpec
