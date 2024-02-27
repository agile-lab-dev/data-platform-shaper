package it.agilelab.dataplatformshaper.domain

import cats.data.EitherT
import cats.effect.{IO, Ref}
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.dataplatformshaper.domain.model.l0
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  MappingManagementServiceIntepreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}

import scala.language.{dynamics, implicitConversions}

class MappingSpec extends CommonSpec:

  given cache: Ref[IO, Map[String, EntityType]] =
    Ref[IO].of(Map.empty[String, EntityType]).unsafeRunSync()

  val schema1: Schema = StructType(
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

  val dataCollectionType =
    EntityType("DataCollectionType", Set("MappingSource"), schema1)

  val schema2: Schema = StructType(
    List(
      "bucketName" -> StringType(),
      "folderPath" -> StringType(),
      "anInt" -> IntType()
    )
  )

  val s3FolderType =
    EntityType("S3FolderType", Set("MappingTarget"), schema2)

  /*
  val dataCollectionTypeInstance = Entity(
    "",
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
  )*/

  val mapperTuple = (
    "bucketName" -> "'MyBucket'",
    "folderPath" ->
      s"""
         |instance.get('organization') += '/' += instance.get('sub-organization')
         |""".stripMargin,
    "anInt" -> "instance.get('nested/nestedField1')"
  )

  "Creating a Mapping instance" - {
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
          res3 <- EitherT(
            mservice.create(
              "mapping1",
              "DataCollectionType",
              "S3FolderType",
              mapperTuple
            )
          )
        } yield (res1, res2, res3)).value

      } asserting (ret =>
        ret should matchPattern { case Right(((), (), ())) => }
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
        val mservice = MappingManagementServiceIntepreter[IO](tservice)

        (for {
          res <- EitherT(
            mservice.create(
              "mapping1",
              "DataCollectionType",
              "S3FolderType",
              mapperTuple
            )
          )
        } yield res).value

      } asserting (ret => ret should matchPattern { case Left(_) => })
    }
  }

end MappingSpec
