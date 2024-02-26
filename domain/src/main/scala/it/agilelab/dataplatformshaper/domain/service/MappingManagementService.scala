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

  def delete(mappingName: String): F[Either[ManagementServiceError, Unit]]

end MappingManagementService
