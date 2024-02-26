package it.agilelab.dataplatformshaper.domain.service

import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.l0.EntityType

trait MappingManagementService[F[_]]:

  val repository: KnowledgeGraph[F]

  def create(
      mappingName: String,
      sourceEntityType: EntityType,
      targetEntityType: EntityType,
      mapper: Tuple
  ): F[Either[ManagementServiceError, Unit]]

  def delete(mappingName: String): F[Either[ManagementServiceError, Unit]]

end MappingManagementService
