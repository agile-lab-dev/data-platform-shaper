package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.effect.Sync
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.l1.Relationship
import it.agilelab.dataplatformshaper.domain.service.{
  ManagementServiceError,
  TraitManagementService
}

class TraitManagementServiceInterpreter[F[_]: Sync](
    val repository: KnowledgeGraph[F]
) extends TraitManagementService[F]:

  override def create(
      traitName: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def exist(
      traitName: String
  ): F[Either[ManagementServiceError, Boolean]] = ???

  override def link(
      trait1Name: String,
      linkType: Relationship,
      traitName2: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def unlink(
      trait1Name: String,
      linkType: Relationship,
      trait2Name: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def linked(
      traitName: String,
      linkType: Relationship
  ): F[Either[ManagementServiceError, List[String]]] = ???

end TraitManagementServiceInterpreter
