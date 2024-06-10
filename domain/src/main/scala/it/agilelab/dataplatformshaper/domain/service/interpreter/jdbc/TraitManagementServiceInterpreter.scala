package it.agilelab.dataplatformshaper.domain.service.interpreter.jdbc

import cats.effect.Sync
import it.agilelab.dataplatformshaper.domain.model.{BulkTraitsCreationRequest, BulkTraitsCreationResponse, Relationship, Trait}
import it.agilelab.dataplatformshaper.domain.service.{ManagementServiceError, TraitManagementService}

class TraitManagementServiceInterpreter[F[_]: Sync]() extends TraitManagementService[F]:
  override def create(traitDefinition: Trait): F[Either[ManagementServiceError, Unit]] = ???

  override def create(bulkTraitsCreationRequest: BulkTraitsCreationRequest): F[BulkTraitsCreationResponse] = ???

  override def delete(traitName: String): F[Either[ManagementServiceError, Unit]] = ???

  override def exist(traitName: String): F[Either[ManagementServiceError, Boolean]] = ???

  override def list(): F[Either[ManagementServiceError, List[String]]] = ???

  override def exist(traitNames: Set[String]): F[Either[ManagementServiceError, Set[(String, Boolean)]]] = ???

  override def link(trait1Name: String, linkType: Relationship, traitName2: String): F[Either[ManagementServiceError, Unit]] = ???

  override def unlink(trait1Name: String, linkType: Relationship, trait2Name: String): F[Either[ManagementServiceError, Unit]] = ???

  override def linked(traitName: String, linkType: Relationship): F[Either[ManagementServiceError, List[String]]] = ???
end TraitManagementServiceInterpreter
