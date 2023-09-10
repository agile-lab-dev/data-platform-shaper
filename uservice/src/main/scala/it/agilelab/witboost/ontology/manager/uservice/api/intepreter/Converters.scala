package it.agilelab.witboost.ontology.manager.uservice.api.intepreter

import it.agilelab.witboost.ontology.manager.domain.model.schema.*
import it.agilelab.witboost.ontology.manager.uservice.definitions.{
  AttributeTypeName,
  AttributeType as OpenApiAttributeType,
  Mode as OpenApiMode
}
import org.datatools.bigdatatypes.basictypes.SqlType

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

given OpenApiAttributeTypeToAttributeType
    : Conversion[OpenApiAttributeType, (String, DataType)] with
  def apply(oaAttributeType: OpenApiAttributeType): (String, DataType) =
    oaAttributeType.typeName match
      case AttributeTypeName.members.String =>
        (
          oaAttributeType.name,
          StringType(
            oaAttributeType.mode.getOrElse(OpenApiMode.members.Required)
          )
        )
      case AttributeTypeName.members.Integer =>
        (
          oaAttributeType.name,
          IntType(oaAttributeType.mode.getOrElse(OpenApiMode.members.Required))
        )
    end match
  end apply

given OpenApiSchemaToSchema: Conversion[OpenApiSchema, Schema] with
  def apply(oaSchema: OpenApiSchema): Schema =
    StructType(
      oaSchema
        .map(oaAttributeType => oaAttributeType: (String, DataType))
        .toList
    )
  end apply
