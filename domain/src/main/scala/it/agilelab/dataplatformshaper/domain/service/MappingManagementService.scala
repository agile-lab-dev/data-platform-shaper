package it.agilelab.dataplatformshaper.domain.service

import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.mapping.{
  MappingKey,
  MappingDefinition
}

trait MappingManagementService[F[_]]:

  val repository: KnowledgeGraph[F]

  def create(
      mappingDefinition: MappingDefinition
  ): F[Either[ManagementServiceError, Unit]]

  def exist(mapperKey: MappingKey): F[Either[ManagementServiceError, Boolean]]

  def createMappedInstances(
      sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]]

end MappingManagementService
