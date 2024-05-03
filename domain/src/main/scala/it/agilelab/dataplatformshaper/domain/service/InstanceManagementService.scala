package it.agilelab.dataplatformshaper.domain.service

import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.{Entity, Relationship}
import it.agilelab.dataplatformshaper.domain.model.schema.SearchPredicate

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

  def list(
      instanceTypeName: String,
      predicate: Option[SearchPredicate],
      returnEntities: Boolean,
      limit: Option[Int]
  ): F[Either[ManagementServiceError, List[String | Entity]]]

  def list(
      instanceTypeName: String,
      predicate: String,
      returnEntities: Boolean,
      limit: Option[Int]
  ): F[Either[ManagementServiceError, List[String | Entity]]]

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
