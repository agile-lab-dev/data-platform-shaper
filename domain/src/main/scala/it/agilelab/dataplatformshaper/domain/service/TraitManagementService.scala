package it.agilelab.dataplatformshaper.domain.service

import it.agilelab.dataplatformshaper.domain.model.{
  BulkTraitsCreationRequest,
  BulkTraitsCreationResponse,
  Relationship,
  Trait
}

trait TraitManagementService[F[_]]:
  
  def create(traitDefinition: Trait): F[Either[ManagementServiceError, Unit]]

  def create(
    bulkTraitsCreationRequest: BulkTraitsCreationRequest
  ): F[BulkTraitsCreationResponse]

  def delete(traitName: String): F[Either[ManagementServiceError, Unit]]

  def exist(traitName: String): F[Either[ManagementServiceError, Boolean]]

  def list(): F[Either[ManagementServiceError, List[String]]]

  def exist(
    traitNames: Set[String]
  ): F[Either[ManagementServiceError, Set[(String, Boolean)]]]

  def link(
    trait1Name: String,
    linkType: Relationship,
    traitName2: String
  ): F[Either[ManagementServiceError, Unit]]

  def unlink(
    trait1Name: String,
    linkType: Relationship,
    trait2Name: String
  ): F[Either[ManagementServiceError, Unit]]

  def linked(
    traitName: String,
    linkType: Relationship
  ): F[Either[ManagementServiceError, List[String]]]

end TraitManagementService
