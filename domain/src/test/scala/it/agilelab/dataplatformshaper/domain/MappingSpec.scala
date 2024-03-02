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
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  MappingManagementServiceIntepreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}

import scala.concurrent.duration.*
import scala.language.{dynamics, implicitConversions}

class MappingSpec extends CommonSpec:

  given cache: Cache[IO, String, EntityType] = CaffeineCache
    .build[IO, String, EntityType](
      Some(TimeSpec.unsafeFromDuration(1.second)),
      None,
      None
    )
    .unsafeRunSync()

  private val schema1: Schema = StructType(
    List(
      "name" -> StringType(),
      "value" -> StringType(),
      "organization" -> StringType(),
      "sub-organization" -> StringType(),
      "domain" -> StringType(),
      "sub-domain" -> StringType(),
      "nested" -> StructType(
        List(
          "nestedField1" -> IntType(),
          "nestedField2" -> IntType()
        )
      )
    )
  )

  private val dataCollectionType =
    EntityType("DataCollectionType", Set("MappingSource"), schema1)

  private val schema2: Schema = StructType(
    List(
      "bucketName" -> StringType(),
      "folderPath" -> StringType(),
      "anInt" -> IntType()
    )
  )

  private val s3FolderType =
    EntityType("S3FolderType", Set("MappingSource", "MappingTarget"), schema2)

  private val schema3: Schema = StructType(
    List(
      "roleName" -> StringType()
    )
  )

  private val iamRoleType =
    EntityType("IAMRoleType", Set("MappingTarget"), schema3)

  private val mapperTuple1 = (
    "bucketName" -> "'MyBucket'",
    "folderPath" ->
      s"""
         |instance.get('organization') += '/' += instance.get('sub-organization')
         |""".stripMargin,
    "anInt" -> "instance.get('nested/nestedField1')"
  )

  private val mapperTuple2 = Tuple1(
    "roleName" -> "instance.get('bucketName') += 'IamRole'"
  )

  "Creating a mapping instance" - {
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
        val mservice = MappingManagementServiceIntepreter[IO](tservice)

        (for {
          res1 <- EitherT(tservice.create(dataCollectionType))
          res2 <- EitherT(tservice.create(s3FolderType))
          res3 <- EitherT(tservice.create(iamRoleType))
          res4 <- EitherT(
            mservice.create(
              "mapping1",
              "DataCollectionType",
              "S3FolderType",
              mapperTuple1
            )
          )
          res5 <- EitherT(
            mservice.create(
              "mapping2",
              "S3FolderType",
              "IAMRoleType",
              mapperTuple2
            )
          )

        } yield (res1, res2, res3, res4, res5)).value

      } asserting (ret =>
        ret should matchPattern { case Right(((), (), (), (), ())) => }
      )
    }
  }

  "Automatic creation of instances driven by the mapping" - {
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

        (for {
          res1 <- EitherT(
            iservice.create(
              "DataCollectionType",
              (
                "name" -> "Person",
                "value" -> "Bronze",
                "organization" -> "HR",
                "sub-organization" -> "Any",
                "domain" -> "People",
                "sub-domain" -> "Registration",
                "nested" -> (
                  "nestedField1" -> 1,
                  "nestedField2" -> 2
                )
              )
            )
          )
        } yield res1).value

      } asserting (ret => ret should matchPattern { case Right(_) => })
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
        val mservice = MappingManagementServiceIntepreter[IO](tservice)

        (for {
          res <- EitherT(
            mservice.create(
              "mapping1",
              "DataCollectionType",
              "S3FolderType",
              mapperTuple1
            )
          )
        } yield res).value

      } asserting (ret => ret should matchPattern { case Left(_) => })
    }
  }

end MappingSpec
