package it.agilelab.dataplatformshaper.domain.service

import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.EntityType

trait TypeManagementService[F[_]]:

  val repository: KnowledgeGraph[F]

  def create(entityType: EntityType): F[Either[ManagementServiceError, Unit]]

  def create(
    entityType: EntityType,
    inheritsFrom: String
  ): F[Either[ManagementServiceError, Unit]]

  def read(
    instanceTypeName: String
  ): F[Either[ManagementServiceError, EntityType]]

  def updateConstraints(
    entityType: EntityType
  ): F[Either[ManagementServiceError, Unit]]

  def delete(instanceTypeName: String): F[Either[ManagementServiceError, Unit]]

  def exist(
    instanceTypeName: String
  ): F[Either[ManagementServiceError, Boolean]]

  def list(): F[Either[ManagementServiceError, List[EntityType]]]

end TypeManagementService
