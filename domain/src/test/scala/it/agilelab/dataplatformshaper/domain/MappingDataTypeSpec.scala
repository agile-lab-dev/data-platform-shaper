package it.agilelab.dataplatformshaper.domain

import it.agilelab.dataplatformshaper.domain.model.Entity
import it.agilelab.dataplatformshaper.domain.model.schema.DataType.*
import it.agilelab.dataplatformshaper.domain.model.schema.{
  Schema,
  tupleToMappedTuple,
  validateMappingTuple
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.language.{dynamics, implicitConversions}

class MappingDataTypeSpec extends AnyFlatSpec with Matchers:

  behavior of "MappingDataType"

  "Mapping a type into another type" should "work" in {

    val schema1: Schema = StructType(
      List(
        "name" -> StringType(),
        "value" -> StringType(),
        "organization" -> StringType(),
        "sub-organization" -> StringType(),
        "domain" -> StringType(),
        "sub-domain" -> StringType(),
        "nested" -> StructType(
          List("nestedField1" -> IntType(), "nestedField2" -> IntType())
        )
      )
    )

    val schema2: Schema = StructType(
      List(
        "bucketName" -> StringType(),
        "folderPath" -> StringType(),
        "anInt" -> IntType()
      )
    )

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
    )

    val mappingTuple = (
      "bucketName" -> "'MyBucket'",
      "folderPath" -> s"""
                         |source.get('organization') += '/' += source.get('sub-organization')
                         |""".stripMargin,
      "anInt" -> "source.get('nested/nestedField1') + 10"
    )

    val mres = validateMappingTuple(mappingTuple, schema2)
    val _ = mres should matchPattern { case Right(()) => }

    val res = tupleToMappedTuple(
      dataCollectionTypeInstance.values,
      schema1,
      mappingTuple,
      schema2,
      Map.empty[String, Tuple]
    )
    res should matchPattern {
      case Right(
            (
              ("bucketName", "MyBucket"),
              ("folderPath", "HR/Any"),
              ("anInt", 11)
            )
          ) =>
    }

  }
end MappingDataTypeSpec
