package it.agilelab.dataplatformshaper.uservice.api.intepreter

import it.agilelab.dataplatformshaper.domain.model.l0.EntityType
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.uservice.definitions.{
  AttributeTypeName,
  AttributeType as OpenApiAttributeType,
  EntityType as OpenApiEntityType,
  Mode as OpenApiMode
}

import scala.language.implicitConversions

type OpenApiSchema = Vector[OpenApiAttributeType]

given OpenApiModeToMode: Conversion[OpenApiMode, Mode] with
  def apply(oaMode: OpenApiMode): Mode =
    oaMode match
      case OpenApiMode.members.Nullable => Mode.Nullable
      case OpenApiMode.members.Repeated => Mode.Repeated
      case OpenApiMode.members.Required => Mode.Required
    end match
  end apply

given ModeToOpenApiMode: Conversion[Mode, OpenApiMode] with
  def apply(mode: Mode): OpenApiMode =
    mode match
      case Mode.Nullable => OpenApiMode.members.Nullable
      case Mode.Repeated => OpenApiMode.members.Repeated
      case Mode.Required => OpenApiMode.members.Required
    end match
  end apply

given OpenApiAttributeTypeToAttributeType
    : Conversion[OpenApiAttributeType, (String, DataType)] with
  def apply(oaAttributeType: OpenApiAttributeType): (String, DataType) =
    oaAttributeType.typeName match
      case AttributeTypeName.members.String =>
        (
          oaAttributeType.name,
          StringType(
            oaAttributeType.mode.getOrElse(OpenApiMode.members.Required),
            oaAttributeType.constraints
          )
        )
      case AttributeTypeName.members.Integer =>
        (
          oaAttributeType.name,
          IntType(
            oaAttributeType.mode.getOrElse(OpenApiMode.members.Required),
            oaAttributeType.constraints
          )
        )
      case AttributeTypeName.members.Long =>
        (
          oaAttributeType.name,
          LongType(
            oaAttributeType.mode.getOrElse(OpenApiMode.members.Required),
            oaAttributeType.constraints
          )
        )
      case AttributeTypeName.members.Date =>
        (
          oaAttributeType.name,
          DateType(
            oaAttributeType.mode.getOrElse(OpenApiMode.members.Required),
            oaAttributeType.constraints
          )
        )
      case AttributeTypeName.members.Timestamp =>
        (
          oaAttributeType.name,
          TimestampType(
            oaAttributeType.mode.getOrElse(OpenApiMode.members.Required),
            oaAttributeType.constraints
          )
        )
      case AttributeTypeName.members.Double =>
        (
          oaAttributeType.name,
          DoubleType(
            oaAttributeType.mode.getOrElse(OpenApiMode.members.Required),
            oaAttributeType.constraints
          )
        )
      case AttributeTypeName.members.Float =>
        (
          oaAttributeType.name,
          FloatType(
            oaAttributeType.mode.getOrElse(OpenApiMode.members.Required),
            oaAttributeType.constraints
          )
        )
      case AttributeTypeName.members.Bool =>
        (
          oaAttributeType.name,
          BooleanType(
            oaAttributeType.mode.getOrElse(OpenApiMode.members.Required),
            oaAttributeType.constraints
          )
        )
      case AttributeTypeName.members.Json =>
        (
          oaAttributeType.name,
          JsonType(
            oaAttributeType.mode.getOrElse(OpenApiMode.members.Required)
          )
        )
      case AttributeTypeName.members.Struct =>
        (
          oaAttributeType.name,
          StructType(
            oaAttributeType.attributeTypes
              .getOrElse(List.empty[OpenApiAttributeType])
              .map(apply)
              .toList,
            oaAttributeType.mode.getOrElse(OpenApiMode.members.Required)
          )
        )
    end match
  end apply

given AttributeTypeToOpenApiAttributeType
    : Conversion[(String, DataType), OpenApiAttributeType] with
  def apply(pair: (String, DataType)): OpenApiAttributeType =
    val name = pair(0)
    val oaAttributeType =
      pair(1) match
        case StringType(mode, constraints) =>
          OpenApiAttributeType(
            name,
            AttributeTypeName.String,
            Some(mode),
            constraints,
            None
          )
        case IntType(mode, constraints) =>
          OpenApiAttributeType(
            name,
            AttributeTypeName.Integer,
            Some(mode),
            constraints,
            None
          )
        case LongType(mode, constraints) =>
          OpenApiAttributeType(
            name,
            AttributeTypeName.Long,
            Some(mode),
            constraints,
            None
          )
        case FloatType(mode, constraints) =>
          OpenApiAttributeType(
            name,
            AttributeTypeName.Float,
            Some(mode),
            constraints,
            None
          )
        case DoubleType(mode, constraints) =>
          OpenApiAttributeType(
            name,
            AttributeTypeName.Double,
            Some(mode),
            constraints,
            None
          )
        case BooleanType(mode, constraints) =>
          OpenApiAttributeType(
            name,
            AttributeTypeName.Bool,
            Some(mode),
            constraints,
            None
          )
        case DateType(mode, constraints) =>
          OpenApiAttributeType(
            name,
            AttributeTypeName.Date,
            Some(mode),
            constraints,
            None
          )
        case TimestampType(mode, constraints) =>
          OpenApiAttributeType(
            name,
            AttributeTypeName.Timestamp,
            Some(mode),
            constraints,
            None
          )
        case JsonType(mode) =>
          OpenApiAttributeType(
            name,
            AttributeTypeName.Json,
            Some(mode),
            None,
            None
          )
        case StructType(attributes, mode) =>
          OpenApiAttributeType(
            name,
            AttributeTypeName.Struct,
            Some(mode),
            None,
            Some(attributes.map(apply).toVector)
          )
        case _ =>
          OpenApiAttributeType(
            name,
            AttributeTypeName.String,
            Some(OpenApiMode.Required),
            None
          )
      end match
    oaAttributeType
  end apply

given OpenApiSchemaToSchema: Conversion[OpenApiSchema, Schema] with
  def apply(oaSchema: OpenApiSchema): Schema =
    StructType(
      oaSchema
        .map(oaAttributeType => oaAttributeType: (String, DataType))
        .toList
    )
  end apply

given SchemaToOpenApiSchema: Conversion[Schema, OpenApiSchema] with
  def apply(schema: Schema): OpenApiSchema =
    schema.records.map(pair => pair: OpenApiAttributeType).toVector
  end apply

given EntityTypeToOpenApiEntityType: Conversion[EntityType, OpenApiEntityType]
with
  def apply(entityType: EntityType): OpenApiEntityType =
    val name: String = entityType.name
    val traits: Vector[String] = entityType.traits.toVector.map(t => t: String)
    val schema: OpenApiSchema = entityType.baseSchema
    val fatherName: Option[String] = entityType.father.map(_.name)
    OpenApiEntityType(name, Some(traits), schema, fatherName)
  end apply
