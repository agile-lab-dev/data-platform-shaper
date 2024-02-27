package it.agilelab.dataplatformshaper.domain.service

import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph

trait MappingManagementService[F[_]]:

  val repository: KnowledgeGraph[F]

  def create(
      mappingName: String,
      sourceEntityTypeName: String,
      targetEntityTypeName: String,
      mapper: Tuple
  ): F[Either[ManagementServiceError, Unit]]

  def exist(
      mappingName: String,
      sourceEntityTypeName: String,
      targetEntityTypeName: String
  ): F[Either[ManagementServiceError, Boolean]]

end MappingManagementService
