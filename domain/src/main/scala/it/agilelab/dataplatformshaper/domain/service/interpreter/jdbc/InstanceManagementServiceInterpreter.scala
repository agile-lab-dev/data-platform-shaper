package it.agilelab.dataplatformshaper.domain.service.interpreter.jdbc

import cats.effect.Sync
import it.agilelab.dataplatformshaper.domain.model.schema.SearchPredicate
import it.agilelab.dataplatformshaper.domain.model.{Entity, Relationship}
import it.agilelab.dataplatformshaper.domain.service.{
  InstanceManagementService,
  ManagementServiceError
}

class InstanceManagementServiceInterpreter[F[_]: Sync](
  typeManagementService: TypeManagementServiceInterpreter[F]
) extends InstanceManagementService[F] {
  print(s"$typeManagementService") // TODO: remove after implementation

  override def create(
    instanceTypeName: String,
    values: Tuple
  ): F[Either[ManagementServiceError, String]] = ???

  override def read(
    instanceId: String
  ): F[Either[ManagementServiceError, Entity]] = ???

  override def update(
    instanceId: String,
    values: Tuple
  ): F[Either[ManagementServiceError, String]] = ???

  override def delete(
    instanceId: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def exist(
    instanceId: String
  ): F[Either[ManagementServiceError, Boolean]] = ???

  override def list(
    instanceTypeName: String,
    predicate: Option[SearchPredicate],
    returnEntities: Boolean,
    limit: Option[Int]
  ): F[Either[ManagementServiceError, List[String | Entity]]] = ???

  override def list(
    instanceTypeName: String,
    predicate: String,
    returnEntities: Boolean,
    limit: Option[Int]
  ): F[Either[ManagementServiceError, List[String | Entity]]] = ???

  override def link(
    instanceId1: String,
    linkType: Relationship,
    instanceId2: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def unlink(
    instanceId1: String,
    linkType: Relationship,
    instanceId2: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def linked(
    instanceId: String,
    linkType: Relationship
  ): F[Either[ManagementServiceError, List[String]]] = ???
}
