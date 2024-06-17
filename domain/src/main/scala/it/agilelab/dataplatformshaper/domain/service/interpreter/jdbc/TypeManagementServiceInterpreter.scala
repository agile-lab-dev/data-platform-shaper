package it.agilelab.dataplatformshaper.domain.service.interpreter.jdbc

import cats.effect.Sync
import it.agilelab.dataplatformshaper.domain.model.EntityType
import it.agilelab.dataplatformshaper.domain.service.{
  ManagementServiceError,
  TypeManagementService
}

class TypeManagementServiceInterpreter[F[_]: Sync](
  traitManagementService: TraitManagementServiceInterpreter[F]
) extends TypeManagementService[F]:
  print(s"$traitManagementService") // TODO Remove after implementing
  override def create(
    entityType: EntityType
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def create(
    entityType: EntityType,
    inheritsFrom: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def read(
    instanceTypeName: String
  ): F[Either[ManagementServiceError, EntityType]] = ???

  override def updateConstraints(
    entityType: EntityType
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def delete(
    instanceTypeName: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def exist(
    instanceTypeName: String
  ): F[Either[ManagementServiceError, Boolean]] = ???

  override def list(): F[Either[ManagementServiceError, List[EntityType]]] = ???

end TypeManagementServiceInterpreter
