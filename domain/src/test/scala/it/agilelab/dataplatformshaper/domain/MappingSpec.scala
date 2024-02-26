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
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import org.scalactic.Equality

import scala.language.{dynamics, implicitConversions}

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.asInstanceOf",
    "scalafix:DisableSyntax.isInstanceOf",
    "scalafix:DisableSyntax.=="
  )
)
class MappingSpec extends CommonSpec:

  given Equality[DataType] with
    def areEqual(x: DataType, y: Any): Boolean =
      x match
        case struct: StructType if y.isInstanceOf[StructType] =>
          struct === y.asInstanceOf[StructType]
        case _ =>
          x == y
    end areEqual
  end given

  given Equality[StructType] with
    def areEqual(x: StructType, y: Any): Boolean =
      val c1: Map[String, DataType] = x.records.toMap
      val c2: Map[String, DataType] = y.asInstanceOf[StructType].records.toMap
      val ret = c1.foldLeft(true)((b, p) => b && c2(p(0)) === p(1))
      ret
    end areEqual
  end given

  given cache: Ref[IO, Map[String, EntityType]] =
    Ref[IO].of(Map.empty[String, EntityType]).unsafeRunSync()

  val schema1: Schema = StructType(
    List(
      "name" -> StringType(),
      "value" -> StringType(),
      "organization" -> StringType(),
      "sub-organization" -> StringType(),
      "domain" -> StringType(),
      "sub-domain" -> StringType()
    )
  )

  val schema2: Schema = StructType(
    List(
      "bucketName" -> StringType(),
      "folderPath" -> StringType()
    )
  )

  "Creating an EntityType instance" - {
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

        /*
        val mapperTuple  = (
          "bucketName" -> "'MyBucket'",
          "folderPath" ->
            s"""
               |instance.get('organization') += '/' += instance.get('sub-organization')
               |""".stripMargin,
          "anInt" -> "instance.get('nested/nestedField1')"
        )*/

        (for {
          res1 <- EitherT(tservice.create(dataCollectionType))
          res2 <- EitherT(tservice.create(s3FolderType))
        } yield (res1, res2)).value

      } asserting (ret => ret should matchPattern { case Right(((), ())) => })
    }
  }

end MappingSpec
