package it.agilelab.dataplatformshaper.domain.service

import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.mapping.{
  MappingDefinition,
  MappingKey
}
import it.agilelab.dataplatformshaper.domain.model.{Entity, EntityType}

trait MappingManagementService[F[_]]:

  val repository: KnowledgeGraph[F]

  def create(
    mappingDefinition: MappingDefinition
  ): F[Either[ManagementServiceError, Unit]]

  def read(
    mappingKey: MappingKey
  ): F[Either[ManagementServiceError, MappingDefinition]]

  def update(
    mappingKey: MappingKey,
    mapper: Tuple
  ): F[Either[ManagementServiceError, Unit]]

  def delete(mappingKey: MappingKey): F[Either[ManagementServiceError, Unit]]

  def exist(mapperKey: MappingKey): F[Either[ManagementServiceError, Boolean]]

  def createMappedInstances(
    sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]]

  def readMappedInstances(
    sourceInstanceId: String
  ): F[Either[ManagementServiceError, List[
    ((EntityType, Entity), String, (EntityType, Entity))
  ]]]

  def updateMappedInstances(
    sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]]

  def deleteMappedInstances(
    sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]]

  def readTargetInstance(
    sourceInstanceId: String,
    mappingName: String
  ): F[Either[ManagementServiceError, Option[Entity]]]

end MappingManagementService
