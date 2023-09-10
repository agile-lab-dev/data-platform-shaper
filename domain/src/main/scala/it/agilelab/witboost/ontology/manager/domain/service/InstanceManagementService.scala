package it.agilelab.witboost.ontology.manager.domain.service

import it.agilelab.witboost.ontology.manager.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.witboost.ontology.manager.domain.model.l0.Entity
import it.agilelab.witboost.ontology.manager.domain.model.l1.Relationship

trait InstanceManagementService[F[_]]:

  val repository: KnowledgeGraph[F]

  def create(
      instanceTypeName: String,
      values: Tuple
  ): F[Either[ManagementServiceError, String]]

  def read(instanceId: String): F[Either[ManagementServiceError, Entity]]

  def update(
      instanceId: String,
      values: Tuple
  ): F[Either[ManagementServiceError, String]]

  def delete(
      instanceId: String
  ): F[Either[ManagementServiceError, Unit]]

  def exist(instanceId: String): F[Either[ManagementServiceError, Boolean]]

  def link(
      instanceId1: String,
      linkType: Relationship,
      instanceId2: String
  ): F[Either[ManagementServiceError, Unit]]

  def unlink(
      instanceId1: String,
      linkType: Relationship,
      instanceId2: String
  ): F[Either[ManagementServiceError, Unit]]

  def linked(
      instanceId: String,
      linkType: Relationship
  ): F[Either[ManagementServiceError, List[String]]]

end InstanceManagementService
