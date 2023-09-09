package it.agilelab.witboost.ontology.manager.domain.service

import it.agilelab.witboost.ontology.manager.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.witboost.ontology.manager.domain.model.l0.EntityType

@SuppressWarnings(Array("org.wartremover.warts.Overloading"))
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

  def exist(
      instanceTypeName: String
  ): F[Either[ManagementServiceError, Boolean]]

end TypeManagementService
