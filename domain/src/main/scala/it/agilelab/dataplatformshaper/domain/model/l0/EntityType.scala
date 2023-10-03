package it.agilelab.dataplatformshaper.domain.model.l0

import it.agilelab.dataplatformshaper.domain.model.l1.{
  GenericTrait,
  SpecificTrait
}
import it.agilelab.dataplatformshaper.domain.model.schema.Schema

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.var"
  )
)
final case class EntityType(
    name: String,
    traits: Set[SpecificTrait],
    baseSchema: Schema,
    father: Option[EntityType]
):
  def schema: Schema =
    var sc = baseSchema
    father.foreach(et =>
      val scSchemaFieldNames = sc.records.map(_(0)).toSet
      sc = sc.copy(records =
        et.schema.records.filter(r => !scSchemaFieldNames(r(0))) ++ sc.records
      )
    )
    traits.foreach(
      _ match
        case tr: GenericTrait =>
          val traitSchema = tr.schema
          val scSchemaFieldNames = sc.records.map(_(0)).toSet
          sc = sc.copy(records =
            traitSchema.records.filter(r =>
              !scSchemaFieldNames(r(0))
            ) ++ sc.records
          )
        case _ =>
    )
    sc
  end schema

end EntityType

object EntityType:
  def apply(
      name: String,
      traits: Set[SpecificTrait],
      initialSchema: Schema
  ): EntityType = EntityType(name, traits, initialSchema, None)
  def apply(
      name: String,
      traits: Set[SpecificTrait],
      initialSchema: Schema,
      fatherEntityType: EntityType
  ): EntityType =
    EntityType(name, traits, initialSchema, Some(fatherEntityType))
  def apply(
      name: String,
      initialSchema: Schema,
      fatherEntityType: EntityType
  ): EntityType = EntityType(
    name,
    Set.empty[SpecificTrait],
    initialSchema,
    Some(fatherEntityType)
  )
  def apply(name: String, initialSchema: Schema): EntityType =
    EntityType(name, Set.empty[SpecificTrait], initialSchema, None)

end EntityType
