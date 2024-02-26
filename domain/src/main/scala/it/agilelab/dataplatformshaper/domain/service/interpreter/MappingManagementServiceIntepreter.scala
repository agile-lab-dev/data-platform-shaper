package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.*
import cats.effect.*
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.l0.EntityType
import it.agilelab.dataplatformshaper.domain.service.{
  ManagementServiceError,
  MappingManagementService
}

class MappingManagementServiceIntepreter[F[_]: Sync]
    extends MappingManagementService[F] {

  override val repository: KnowledgeGraph[F] = ???

  override def create(
      mappingName: String,
      sourceEntityType: EntityType,
      targetEntityType: EntityType,
      mapper: Tuple
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def delete(
      mappingName: String
  ): F[Either[ManagementServiceError, Unit]] = ???
}
