package it.agilelab.dataplatformshaper.domain.service

import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.l1.Relationship

trait TraitManagementService[F[_]]:

  val repository: KnowledgeGraph[F]

  def create(
      traitName: String,
      inheritsFrom: Option[String]
  ): F[Either[ManagementServiceError, Unit]]

  def exist(
      traitName: String
  ): F[Either[ManagementServiceError, Boolean]]

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
