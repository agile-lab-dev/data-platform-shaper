package it.agilelab.dataplatformshaper.domain.model

case class BulkTraitsCreationRequest(
  traits: List[Trait],
  relationships: List[(String, Relationship, String)]
)

case class BulkTraitsCreationResponse(
  traits: List[(Trait, Option[String])],
  relationships: List[((String, Relationship, String), Option[String])]
)
