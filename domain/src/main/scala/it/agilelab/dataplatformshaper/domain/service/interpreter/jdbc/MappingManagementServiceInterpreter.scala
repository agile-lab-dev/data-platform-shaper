package it.agilelab.dataplatformshaper.domain.service.interpreter.jdbc

import cats.effect.Sync
import it.agilelab.dataplatformshaper.domain.model.{Entity, EntityType}
import it.agilelab.dataplatformshaper.domain.model.mapping.{
  MappingDefinition,
  MappingKey
}
import it.agilelab.dataplatformshaper.domain.service.{
  ManagementServiceError,
  MappingManagementService
}

class MappingManagementServiceInterpreter[F[_]: Sync](
  typeManagementService: TypeManagementServiceInterpreter[F],
  instanceManagementService: InstanceManagementServiceInterpreter[F]
) extends MappingManagementService[F] {
  print(
    s"$typeManagementService $instanceManagementService"
  ) // TODO: remove after implementation

  override def create(
    mappingDefinition: MappingDefinition
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def read(
    mappingKey: MappingKey
  ): F[Either[ManagementServiceError, MappingDefinition]] = ???

  override def update(
    mappingKey: MappingKey,
    mapper: Tuple
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def delete(
    mappingKey: MappingKey
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def exist(
    mapperKey: MappingKey
  ): F[Either[ManagementServiceError, Boolean]] = ???

  override def createMappedInstances(
    sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def readMappedInstances(
    sourceInstanceId: String
  ): F[Either[ManagementServiceError, List[
    ((EntityType, Entity), String, (EntityType, Entity))
  ]]] = ???

  override def updateMappedInstances(
    sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def deleteMappedInstances(
    sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def readTargetInstance(
    sourceInstanceId: String,
    mappingName: String
  ): F[Either[ManagementServiceError, Option[Entity]]] = ???
}
