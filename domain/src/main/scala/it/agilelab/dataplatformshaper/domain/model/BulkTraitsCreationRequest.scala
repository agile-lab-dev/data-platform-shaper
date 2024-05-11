package it.agilelab.dataplatformshaper.domain.model

case class BulkTraitsCreationRequest(
  traits: List[Trait],
  relationships: List[(String, Relationship, String)]
)
