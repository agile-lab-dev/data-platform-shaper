package it.agilelab.dataplatformshaper.domain.model.mapping

case class MappingKey(
    mappingName: String,
    sourceEntityTypeName: String,
    targetEntityTypeName: String
)

case class MappingDefinition(
    mappingKey: MappingKey,
    mapper: Tuple
)
