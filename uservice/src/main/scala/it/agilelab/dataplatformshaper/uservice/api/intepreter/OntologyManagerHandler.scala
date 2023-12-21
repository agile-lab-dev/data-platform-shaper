package it.agilelab.dataplatformshaper.uservice.api.intepreter

import cats.*
import cats.data.*
import cats.effect.*
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging
import fs2.io.readInputStream
import fs2.{Stream, text}
import io.circe.yaml.parser
import io.circe.yaml.syntax.*
import it.agilelab.dataplatformshaper.domain.model.l0
import it.agilelab.dataplatformshaper.domain.model.l1.{
  Relationship,
  given_Conversion_String_Relationship
}
import it.agilelab.dataplatformshaper.domain.model.l0.{Entity, EntityType}
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import it.agilelab.dataplatformshaper.uservice.Resource.{
  CreateTypeResponse,
  ListEntitiesResponse
}
import it.agilelab.dataplatformshaper.uservice.definitions.{
  QueryRequest,
  Trait,
  ValidationError,
  Entity as OpenApiEntity,
  EntityType as OpenApiEntityType
}
import it.agilelab.dataplatformshaper.uservice.{Handler, Resource}

import java.io.ByteArrayInputStream
import scala.language.implicitConversions
import scala.util.Try

class OntologyManagerHandler[F[_]: Async](
    tms: TypeManagementServiceInterpreter[F],
    ims: InstanceManagementServiceInterpreter[F],
    trms: TraitManagementServiceInterpreter[F]
) extends Handler[F]
    with StrictLogging:

  override def createType(
      respond: Resource.CreateTypeResponse.type
  )(body: OpenApiEntityType): F[CreateTypeResponse] =

    val schema: Schema = body.schema

    val fatherName = body.fatherName

    val traits =
      summon[Applicative[F]].pure(
        Try(
          body.traits
            .fold(Set.empty[String])(x => x.map(str => str).toSet)
        ).toEither
          .leftMap(t => s"Trait ${t.getMessage} is not a Trait")
      )
    val res = for {
      ts <- EitherT(traits)
      res <- EitherT(
        fatherName
          .fold(
            tms
              .create(
                l0.EntityType(
                  body.name,
                  ts,
                  schema,
                  None
                )
              )
          )(fn =>
            tms
              .create(
                l0.EntityType(
                  body.name,
                  ts,
                  schema,
                  None
                ),
                fn
              )
          )
          .map(_.leftMap(_.getMessage))
      )
    } yield res

    res.value
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(_)    => respond.Ok("OK")
      }
      .onError(t =>
        summon[Applicative[F]].pure(logger.error(s"Error: ${t.getMessage}"))
      )
  end createType

  override def createTypeByYaml(
      respond: Resource.CreateTypeByYamlResponse.type
  )(body: Stream[F, Byte]): F[Resource.CreateTypeByYamlResponse] =
    val getEntityType = body
      .through(text.utf8.decode)
      .fold("")(_ + _)
      .compile
      .toList
      .map(_.head)
      .map(parser.parse(_).leftMap(_.getMessage))
      .map(
        _.flatMap(json =>
          OpenApiEntityType.decodeEntityType(json.hcursor).leftMap(_.getMessage)
        )
      )

    val res = for {
      entityType <- EitherT(getEntityType)
      ts = entityType.traits.fold(Set.empty[String])(x =>
        x.map(str => str).toSet
      )
      res <- EitherT(
        entityType.fatherName
          .fold(
            tms
              .create(
                l0.EntityType(
                  entityType.name,
                  ts,
                  entityType.schema,
                  None
                )
              )
          )(fn =>
            tms
              .create(
                l0.EntityType(
                  entityType.name,
                  ts,
                  entityType.schema,
                  None
                ),
                fn
              )
          )
          .map(_.leftMap(_.getMessage))
      )
    } yield res

    res.value
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(_)    => respond.Ok("OK")
      }
      .onError(t =>
        summon[Applicative[F]].pure(logger.error(s"Error: ${t.getMessage}"))
      )
  end createTypeByYaml

  override def readType(respond: Resource.ReadTypeResponse.type)(
      name: String
  ): F[Resource.ReadTypeResponse] =

    val res = for {
      et <- tms
        .read(name)
        .map(_.leftMap(_.getMessage))
    } yield et

    res
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(entityType) => respond.Ok(entityType)
      }
      .onError(t =>
        summon[Applicative[F]].pure(logger.error(s"Error: ${t.getMessage}"))
      )
  end readType

  override def readTypeAsYaml(respond: Resource.ReadTypeAsYamlResponse.type)(
      name: String
  ): F[Resource.ReadTypeAsYamlResponse[F]] =
    val res = (for {
      et <- EitherT(
        tms
          .read(name)
          .map(_.leftMap(_.getMessage))
      )
      stream <- EitherT(
        summon[Applicative[F]].pure(
          Right(
            readInputStream(
              summon[Applicative[F]].pure(
                new ByteArrayInputStream(
                  OpenApiEntityType
                    .encodeEntityType(
                      et
                    )
                    .asYaml
                    .spaces2
                    .getBytes("UTF8")
                )
              ),
              128
            )
          )
        )
      )
    } yield stream).value

    res
      .map {
        case Left(error)   => respond.BadRequest(ValidationError(Vector(error)))
        case Right(stream) => respond.Ok(stream)
      }
      .onError(t =>
        summon[Applicative[F]].pure(logger.error(s"Error: ${t.getMessage}"))
      )
  end readTypeAsYaml

  override def createEntity(respond: Resource.CreateEntityResponse.type)(
      body: OpenApiEntity
  ): F[Resource.CreateEntityResponse] =
    val res = (for {
      schema <- EitherT(
        tms
          .read(body.entityTypeName)
          .map(_.map(_.schema))
          .map(_.leftMap(_.getMessage))
      )
      tuple <- EitherT(
        summon[Applicative[F]]
          .pure(jsonToTuple(body.values, schema).leftMap(_.getMessage))
      )
      entityId <- EitherT(
        ims.create(body.entityTypeName, tuple).map(_.leftMap(_.getMessage))
      )
    } yield entityId).value

    res
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(entityId) => respond.Ok(entityId)
      }
      .onError(t =>
        summon[Applicative[F]].pure(logger.error(s"Error: ${t.getMessage}"))
      )
  end createEntity

  override def deleteEntity(respond: Resource.DeleteEntityResponse.type)(
      deleteId: String
  ): F[Resource.DeleteEntityResponse] =
    ims
      .delete(deleteId)
      .map {
        case Left(error) =>
          respond.BadRequest(ValidationError(Vector(error.getMessage)))
        case Right(_) => respond.Ok("Entity deleted successfully")
      }
      .onError { t =>
        summon[Applicative[F]].pure(logger.error(s"Error: ${t.getMessage}"))
      }
  end deleteEntity

  override def updateEntity(respond: Resource.UpdateEntityResponse.type)(
      updateId: String,
      body: OpenApiEntity
  ): F[Resource.UpdateEntityResponse] =
    val res = (for {
      schema <- EitherT(
        tms
          .read(body.entityTypeName)
          .map(_.map(_.schema))
          .map(_.leftMap(_.getMessage))
      )
      tuple <- EitherT(
        summon[Applicative[F]]
          .pure(jsonToTuple(body.values, schema).leftMap(_.getMessage))
      )
      entityId <- EitherT(
        ims.update(updateId, tuple).map(_.leftMap(_.getMessage))
      )
    } yield entityId).value

    res
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(entityId) => respond.Ok(entityId)
      }
      .onError(t =>
        summon[Applicative[F]].pure(logger.error(s"Error: ${t.getMessage}"))
      )
  end updateEntity

  override def updateEntityByYaml(
      respond: Resource.UpdateEntityByYamlResponse.type
  )(
      id: String,
      body: Stream[F, Byte]
  ): F[Resource.UpdateEntityByYamlResponse] = {
    val getYaml = body
      .through(text.utf8.decode)
      .fold("")(_ + _)
      .compile
      .toList
      .map(_.head)
      .map(parser.parse(_).leftMap(_.getMessage))
      .map(
        _.flatMap(yaml =>
          OpenApiEntity.decodeEntity(yaml.hcursor).leftMap(_.getMessage)
        )
      )

    val res = (for {
      body <- EitherT(getYaml)
      schema <- EitherT(
        tms
          .read(body.entityTypeName)
          .map(_.map(_.schema))
          .map(_.leftMap(_.getMessage))
      )
      tuple <- EitherT(
        summon[Applicative[F]]
          .pure(jsonToTuple(body.values, schema).leftMap(_.getMessage))
      )
      _ <- EitherT(
        ims.update(id, tuple).map(_.leftMap(_.getMessage))
      )
    } yield id).value

    res
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(updatedId) => respond.Ok(updatedId)
      }
      .onError(t =>
        summon[Applicative[F]].pure(logger.error(s"Error: ${t.getMessage}}"))
      )
  }

  override def createEntityByYaml(
      respond: Resource.CreateEntityByYamlResponse.type
  )(body: fs2.Stream[F, Byte]): F[Resource.CreateEntityByYamlResponse] =
    val getEntity = body
      .through(text.utf8.decode)
      .fold("")(_ + _)
      .compile
      .toList
      .map(_.head)
      .map(parser.parse(_).leftMap(_.getMessage))
      .map(
        _.flatMap(json =>
          OpenApiEntity.decodeEntity(json.hcursor).leftMap(_.getMessage)
        )
      )

    val res = (for {
      body <- EitherT(getEntity)
      schema <- EitherT(
        tms
          .read(body.entityTypeName)
          .map(_.map(_.schema))
          .map(_.leftMap(_.getMessage))
      )
      tuple <- EitherT(
        summon[Applicative[F]]
          .pure(jsonToTuple(body.values, schema).leftMap(_.getMessage))
      )
      entityId <- EitherT(
        ims.create(body.entityTypeName, tuple).map(_.leftMap(_.getMessage))
      )
    } yield entityId).value

    res
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(entityId) => respond.Ok(entityId)
      }
      .onError(t =>
        summon[Applicative[F]].pure(logger.error(s"Error: ${t.getMessage}"))
      )
  end createEntityByYaml

  override def readEntity(respond: Resource.ReadEntityResponse.type)(
      id: String
  ): F[Resource.ReadEntityResponse] =
    val res = (for {
      et <- EitherT(ims.read(id).map(_.leftMap(_.getMessage)))
      schema <- EitherT(
        tms.read(et.entityTypeName).map(_.map(_.schema).leftMap(_.getMessage))
      )
      values <- EitherT(
        summon[Applicative[F]]
          .pure(tupleToJson(et.values, schema).leftMap(_.getMessage))
      )
      oaEntity <- EitherT(
        summon[Applicative[F]].pure(
          Right(
            OpenApiEntity(et.entityId, et.entityTypeName, values)
          )
        )
      )
    } yield oaEntity).value

    res
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(oaEntity) => respond.Ok(oaEntity)
      }
      .onError(t =>
        summon[Applicative[F]].pure(logger.error(s"Error: ${t.getMessage}"))
      )
  end readEntity

  override def readEntityAsYaml(
      respond: Resource.ReadEntityAsYamlResponse.type
  )(id: String): F[Resource.ReadEntityAsYamlResponse[F]] =
    val res = (for {
      et <- EitherT(ims.read(id).map(_.leftMap(_.getMessage)))
      schema <- EitherT(
        tms.read(et.entityTypeName).map(_.map(_.schema).leftMap(_.getMessage))
      )
      values <- EitherT(
        summon[Applicative[F]]
          .pure(tupleToJson(et.values, schema).leftMap(_.getMessage))
      )
      oaEntity <- EitherT(
        summon[Applicative[F]].pure(
          Right(
            readInputStream(
              summon[Applicative[F]].pure(
                new ByteArrayInputStream(
                  OpenApiEntity
                    .encodeEntity(
                      OpenApiEntity(et.entityId, et.entityTypeName, values)
                    )
                    .asYaml
                    .spaces2
                    .getBytes("UTF8")
                )
              ),
              128
            )
          )
        )
      )
    } yield oaEntity).value

    res
      .map {
        case Left(error)   => respond.BadRequest(ValidationError(Vector(error)))
        case Right(stream) => respond.Ok(stream)
      }
      .onError(t =>
        summon[Applicative[F]].pure(logger.error(s"Error: ${t.getMessage}"))
      )
  end readEntityAsYaml

  override def createTrait(respond: Resource.CreateTraitResponse.type)(
      body: Trait
  ): F[Resource.CreateTraitResponse] =
    val res = trms.create(body.name, Option.empty[String])

    res.map {
      case Left(error) =>
        logger.error(s"Error: ${error.getMessage}")
        respond.BadRequest(ValidationError(Vector(error.getMessage)))
      case Right(_) =>
        respond.Ok("Trait created successfully")
    }
  end createTrait

  override def linkTrait(respond: Resource.LinkTraitResponse.type)(
      trait1: String,
      rel: String,
      trait2: String
  ): F[Resource.LinkTraitResponse] =

    val conversion: Either[Throwable, Relationship] =
      Either.catchNonFatal(summon[Conversion[String, Relationship]].apply(rel))

    val result = for {
      relationship <- EitherT.fromEither[F](conversion)
      linkResult <- EitherT(trms.link(trait1, relationship, trait2).attempt)
    } yield linkResult

    result.value.map {
      case Left(error) =>
        logger.error(
          s"Error linking traits or converting string to Relationship: ${error.getMessage}"
        )
        respond.BadRequest(ValidationError(Vector(error.getMessage)))
      case Right(res) =>
        res match
          case Left(serviceError) =>
            logger.error(
              s"Service error in linking traits: ${serviceError.getMessage}"
            )
            respond.BadRequest(ValidationError(Vector(serviceError.getMessage)))
          case Right(_) =>
            respond.Ok(
              s"Traits $trait1 and $trait2 linked successfully with relationship $rel"
            )
        end match
    }
  end linkTrait

  override def unlinkTrait(respond: Resource.UnlinkTraitResponse.type)(
      trait1: String,
      rel: String,
      trait2: String
  ): F[Resource.UnlinkTraitResponse] =

    val conversion: Either[Throwable, Relationship] =
      Either.catchNonFatal(summon[Conversion[String, Relationship]].apply(rel))

    val result = for {
      relationship <- EitherT.fromEither[F](conversion)
      linkResult <- EitherT(trms.unlink(trait1, relationship, trait2).attempt)
    } yield linkResult

    result.value.map {
      case Left(error) =>
        logger.error(
          s"Error unlinking traits or converting string to Relationship: ${error.getMessage}"
        )
        respond.BadRequest(ValidationError(Vector(error.getMessage)))
      case Right(res) =>
        res match
          case Left(serviceError) =>
            logger.error(
              s"Service error in unlinking traits: ${serviceError.getMessage}"
            )
            respond.BadRequest(ValidationError(Vector(serviceError.getMessage)))
          case Right(_) =>
            respond.Ok(
              s"Traits $trait1 and $trait2 with relationship $rel unlinked successfully"
            )
        end match
    }
  end unlinkTrait

  override def linkedTraits(respond: Resource.LinkedTraitsResponse.type)(
      traitName: String,
      rel: String
  ): F[Resource.LinkedTraitsResponse] =

    val conversion: Either[Throwable, Relationship] =
      Either.catchNonFatal(summon[Conversion[String, Relationship]].apply(rel))

    val result = for {
      relationship <- EitherT.fromEither[F](conversion)
      linkedTraits <- EitherT(trms.linked(traitName, relationship).attempt)
    } yield linkedTraits

    result.value.map {
      case Left(error) =>
        logger.error(
          s"Error retrieving linked traits or converting string to Relationship: ${error.getMessage}"
        )
        respond.BadRequest(ValidationError(Vector(error.getMessage)))
      case Right(res) =>
        res match
          case Left(serviceError) =>
            logger.error(
              s"Service error in getting linked traits: ${serviceError.getMessage}"
            )
            respond.BadRequest(ValidationError(Vector(serviceError.getMessage)))
          case Right(traitsList) =>
            respond.Ok(traitsList.toVector)
        end match
    }
  end linkedTraits

  override def linkEntity(respond: Resource.LinkEntityResponse.type)(
      instanceId1: String,
      rel: String,
      instanceId2: String
  ): F[Resource.LinkEntityResponse] =

    val conversion: Either[Throwable, Relationship] =
      Either.catchNonFatal(summon[Conversion[String, Relationship]].apply(rel))

    val result = for {
      relationship <- EitherT.fromEither[F](conversion)
      linkResult <- EitherT(
        ims.link(instanceId1, relationship, instanceId2).attempt
      )
    } yield linkResult

    result.value.map {
      case Left(error) =>
        logger.error(
          s"Error linking instances or converting string to Relationship: ${error.getMessage}"
        )
        respond.BadRequest(ValidationError(Vector(error.getMessage)))
      case Right(res) =>
        res match
          case Left(serviceError) =>
            logger.error(
              s"Service error in linking traits: ${serviceError.getMessage}"
            )
            respond.BadRequest(ValidationError(Vector(serviceError.getMessage)))
          case Right(_) =>
            respond.Ok(
              s"Instances with ids $instanceId1 and $instanceId2 linked successfully with relationship $rel"
            )
        end match
    }
  end linkEntity

  override def unlinkEntity(respond: Resource.UnlinkEntityResponse.type)(
      instanceId1: String,
      rel: String,
      instanceId2: String
  ): F[Resource.UnlinkEntityResponse] =

    val conversion: Either[Throwable, Relationship] =
      Either.catchNonFatal(summon[Conversion[String, Relationship]].apply(rel))

    val result = for {
      relationship <- EitherT.fromEither[F](conversion)
      linkResult <- EitherT(
        ims.unlink(instanceId1, relationship, instanceId2).attempt
      )
    } yield linkResult

    result.value.map {
      case Left(error) =>
        logger.error(
          s"Error unlinking instances or converting string to Relationship: ${error.getMessage}"
        )
        respond.BadRequest(ValidationError(Vector(error.getMessage)))
      case Right(res) =>
        res match
          case Left(serviceError) =>
            logger.error(
              s"Service error in getting linked traits: ${serviceError.getMessage}"
            )
            respond.BadRequest(ValidationError(Vector(serviceError.getMessage)))
          case Right(_) =>
            respond.Ok(
              s"Instances with ids $instanceId1 and $instanceId2 and with relationship $rel unlinked successfully"
            )
        end match
    }
  end unlinkEntity

  override def linkedEntities(respond: Resource.LinkedEntitiesResponse.type)(
      instanceId: String,
      rel: String
  ): F[Resource.LinkedEntitiesResponse] =

    val conversion: Either[Throwable, Relationship] =
      Either.catchNonFatal(summon[Conversion[String, Relationship]].apply(rel))

    val result = for {
      relationship <- EitherT.fromEither[F](conversion)
      linkedEntities <- EitherT(ims.linked(instanceId, relationship).attempt)
    } yield linkedEntities

    result.value.map {
      case Left(error) =>
        logger.error(
          s"Error retrieving linked entities or converting string to Relationship: ${error.getMessage}"
        )
        respond.BadRequest(ValidationError(Vector(error.getMessage)))
      case Right(entities) =>
        entities match {
          case Left(serviceError) =>
            logger.error(
              s"Service error in getting linked entities: ${serviceError.getMessage}"
            )
            respond.BadRequest(ValidationError(Vector(serviceError.getMessage)))
          case Right(entitiesList) =>
            respond.Ok(entitiesList.toVector)
        }
    }
  end linkedEntities

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.throw"
    )
  )
  override def listEntities(respond: Resource.ListEntitiesResponse.type)(
      body: QueryRequest
  ): F[Resource.ListEntitiesResponse] =

    val predicate: Either[Throwable, Option[SearchPredicate]] =
      if body.query.trim.isEmpty then Either.right(None)
      else Either.catchNonFatal(Some(generateSearchPredicate(body.query)))

    val result = for {
      pred <- EitherT.fromEither[F](predicate)
      listEntities <- EitherT(ims.list(body.entityTypeName, pred).attempt)
    } yield listEntities

    tms
      .read(body.entityTypeName)
      .map(_.map(_.schema))
      .map {
        case Left(serviceError) =>
          logger.error(
            s"Error querying instances with type ${body.entityTypeName} and query ${body.query}: ${serviceError.getMessage}"
          )
          summon[Applicative[F]].pure(
            respond.BadRequest(ValidationError(Vector(serviceError.getMessage)))
          )
        case Right(schema) =>
          result.value.map {
            case Left(error) =>
              logger.error(
                s"Error querying instances with type ${body.entityTypeName} and query ${body.query}: ${error.getMessage}"
              )
              respond.BadRequest(ValidationError(Vector(error.getMessage)))
            case Right(entities) =>
              entities match {
                case Left(serviceError) =>
                  logger.error(
                    s"Error querying instances with type ${body.entityTypeName} and query ${body.query}: ${serviceError.getMessage}"
                  )
                  respond.BadRequest(
                    ValidationError(Vector(serviceError.getMessage))
                  )
                case Right(entitiesList) =>
                  val y: ListEntitiesResponse.Ok = respond.Ok(
                    entitiesList.toVector.map(
                      {
                        case et: Entity =>
                          tupleToJson(et.values, schema) match
                            case Left(error) =>
                              logger.error(
                                s"Error querying instances with type ${body.entityTypeName} and query ${body.query}: ${error.getMessage}"
                              )
                              throw new Exception("It shouldn't be here")
                            case Right(json) =>
                              OpenApiEntity(
                                et.entityId,
                                et.entityTypeName,
                                json
                              )
                        case _ => throw new Exception("It shouldn't be here")
                      }
                    )
                  )
                  y
              }
          }
      }
      .flatten
  end listEntities

  override def listEntitiesByIds(
      respond: Resource.ListEntitiesByIdsResponse.type
  )(body: QueryRequest): F[Resource.ListEntitiesByIdsResponse] =
    val predicate: Either[Throwable, Option[SearchPredicate]] =
      if body.query.trim.isEmpty then Either.right(None)
      else Either.catchNonFatal(Some(generateSearchPredicate(body.query)))

    val result = for {
      pred <- EitherT.fromEither[F](predicate)
      listEntities <- EitherT(
        ims.list(body.entityTypeName, pred, false).attempt
      )
    } yield listEntities

    result.value.map {
      case Left(error) =>
        logger.error(
          s"Error querying instances with type ${body.entityTypeName} and query ${body.query}: ${error.getMessage}"
        )
        respond.BadRequest(ValidationError(Vector(error.getMessage)))
      case Right(entities) =>
        entities match {
          case Left(serviceError) =>
            logger.error(
              s"Error querying instances with type ${body.entityTypeName} and query ${body.query}: ${serviceError.getMessage}"
            )
            respond.BadRequest(ValidationError(Vector(serviceError.getMessage)))
          case Right(entitiesList) =>
            respond.Ok(entitiesList.toVector.map {
              case str: String => str
              case _           => "" // TODO
            }: Vector[String])
        }
    }
  end listEntitiesByIds

end OntologyManagerHandler
