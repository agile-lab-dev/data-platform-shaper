package it.agilelab.dataplatformshaper.domain

import it.agilelab.dataplatformshaper.domain.model.l0
import it.agilelab.dataplatformshaper.domain.model.l0.EntityType
import it.agilelab.dataplatformshaper.domain.model.schema.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EntityTypeSpec extends AnyFlatSpec with Matchers:

  behavior of "EntityType"

  val schema0: Schema = StructType(
    List(
      "field0" -> StringType()
    )
  )

  val schema1: Schema = StructType(
    List(
      "field1" -> StringType(),
      "field3" -> StringType()
    )
  )

  val schema2: Schema = StructType(
    List(
      "field1" -> StringType(),
      "field2" -> StringType(),
      "field3" -> StringType(),
      "field4" -> StringType(),
      "field5" -> DateType(),
      "field6" -> TimestampType(),
      "field7" -> DoubleType(),
      "field8" -> FloatType(),
      "field9" -> LongType(),
      "field10" -> BooleanType()
    )
  )

  "Schema inheritance" should "work" in {

    val entityType0 = EntityType("EntityType0", schema0)

    val entityType1 = l0.EntityType("EntityType1", schema1, entityType0)

    val entityType2 = l0.EntityType("EntityType2", schema2, entityType1)

    entityType2.schema shouldBe StructType(
      List(
        "field0" -> StringType(),
        "field1" -> StringType(),
        "field2" -> StringType(),
        "field3" -> StringType(),
        "field4" -> StringType(),
        "field5" -> DateType(),
        "field6" -> TimestampType(),
        "field7" -> DoubleType(),
        "field8" -> FloatType(),
        "field9" -> LongType(),
        "field10" -> BooleanType()
      )
    )

  }

  "Traits inheritance" should "work" in {

    val entityType0 = EntityType("EntityType0", Set("Trait1"), schema0)

    val entityType1 = l0.EntityType(
      "EntityType1",
      Set("Trait1", "Trait2"),
      schema1,
      entityType0
    )

    val entityType2 =
      l0.EntityType("EntityType2", Set("Trait3"), schema2, entityType1)

    entityType2.traits.shouldBe(Set("Trait1", "Trait2", "Trait3"))

  }

end EntityTypeSpec
