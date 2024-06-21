package it.agilelab.dataplatformshaper.domain.service

import cats.effect.Sync
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.model.{
  BulkTraitsCreationRequest,
  BulkTraitsCreationResponse,
  Relationship,
  Trait
}

trait TraitManagementService[F[_]: Sync]:

  def create(traitDefinition: Trait): F[Either[ManagementServiceError, Unit]]

  def create(
    bulkTraitsCreationRequest: BulkTraitsCreationRequest
  ): F[BulkTraitsCreationResponse] =

    val x: F[List[(Trait, Option[String])]] = bulkTraitsCreationRequest.traits
      .map(traitDefinition =>
        this
          .create(traitDefinition)
          .map((traitDefinition, _))
      )
      .sequence
      .map(_.map {
        case (traitDefinition, Left(error)) =>
          (traitDefinition, Some(error.errors.mkString(",")))
        case (traitDefinition, Right(_)) =>
          (traitDefinition, None)
      })

    val y: F[List[((String, Relationship, String), Option[String])]] =
      bulkTraitsCreationRequest.relationships
        .map(rel =>
          this
            .link(rel(0), rel(1), rel(2))
            .map(((rel(0), rel(1): Relationship, rel(2)), _))
        )
        .sequence
        .map(_.map {
          case (linkDefinition, Left(error)) =>
            (linkDefinition, Some(error.errors.mkString(",")))
          case (linkDefinition, Right(_)) =>
            (linkDefinition, None)
        })

    for {
      xr <- x
      yr <- y
    } yield BulkTraitsCreationResponse(xr, yr)
  end create

  def delete(traitName: String): F[Either[ManagementServiceError, Unit]]

  def exist(traitName: String): F[Either[ManagementServiceError, Boolean]]

  def list(): F[Either[ManagementServiceError, List[String]]]

  def exist(
    traitNames: Set[String]
  ): F[Either[ManagementServiceError, Set[(String, Boolean)]]]

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
