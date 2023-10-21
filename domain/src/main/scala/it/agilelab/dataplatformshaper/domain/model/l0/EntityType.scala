package it.agilelab.dataplatformshaper.domain.model.l0

import it.agilelab.dataplatformshaper.domain.model.schema.Schema

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.var"
  )
)
final case class EntityType(
    name: String,
    traits: Set[String],
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
    sc
  end schema

end EntityType

object EntityType:
  def apply(
      name: String,
      traits: Set[String],
      initialSchema: Schema
  ): EntityType = EntityType(name, traits, initialSchema, None)
  def apply(
      name: String,
      traits: Set[String],
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
    Set.empty[String],
    initialSchema,
    Some(fatherEntityType)
  )
  def apply(name: String, initialSchema: Schema): EntityType =
    EntityType(name, Set.empty[String], initialSchema, None)

end EntityType
