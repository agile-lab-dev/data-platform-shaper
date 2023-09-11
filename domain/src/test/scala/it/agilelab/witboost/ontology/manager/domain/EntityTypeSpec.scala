package it.agilelab.witboost.ontology.manager.domain

import it.agilelab.witboost.ontology.manager.domain.model.l0
import it.agilelab.witboost.ontology.manager.domain.model.l0.EntityType
import it.agilelab.witboost.ontology.manager.domain.model.l1.*
import it.agilelab.witboost.ontology.manager.domain.model.schema.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EntityTypeSpec extends AnyFlatSpec with Matchers:

  behavior of "EntityType"

  "Inheritance and traits" should "work" in {

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
        "field4" -> StringType()
      )
    )

    val entityType0 = EntityType("EntityType0", Set(Versionable), schema0)

    val entityType1 = l0.EntityType("EntityType1", schema1, entityType0)

    val entityType2 = l0.EntityType("EntityType2", schema2, entityType1)

    entityType2.schema shouldBe StructType(
      List(
        "version" -> StringType(),
        "field0" -> StringType(),
        "field1" -> StringType(),
        "field2" -> StringType(),
        "field3" -> StringType(),
        "field4" -> StringType()
      )
    )

  }

end EntityTypeSpec
