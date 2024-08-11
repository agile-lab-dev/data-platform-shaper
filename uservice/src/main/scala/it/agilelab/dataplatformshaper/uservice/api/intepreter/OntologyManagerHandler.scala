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
import io.circe.{Json, yaml}
import it.agilelab.dataplatformshaper.domain.model.mapping.{
  MappingDefinition,
  MappingKey
}
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.model.{*, given}
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import it.agilelab.dataplatformshaper.domain.service.{
  ManagementServiceError,
  MappingManagementService
}
import it.agilelab.dataplatformshaper.uservice.Resource.*
import it.agilelab.dataplatformshaper.uservice.definitions.BulkTraitsCreationResponse.Relationships.First
import it.agilelab.dataplatformshaper.uservice.definitions.BulkTraitsCreationResponse.{
  Relationships,
  Traits
}
import it.agilelab.dataplatformshaper.uservice.definitions.{
  MappedInstancesItem,
  ValidationError,
  BulkEntityTypesCreationRequest as OpenApiBulkEntityTypesCreationRequest,
  BulkEntityTypesCreationResponse as OpenApiBulkEntityTypesCreationResponse,
  BulkMappingsCreationRequest as OpenApiBulkMappingsCreationRequest,
  BulkMappingsCreationResponse as OpenApiBulkMappingsCreationResponse,
  BulkTraitsCreationRequest as OpenApiBulkTraitsCreationRequest,
  BulkTraitsCreationResponse as OpenApiBulkTraitsCreationResponse,
  Entity as OpenApiEntity,
  EntityType as OpenApiEntityType,
  MappingDefinition as OpenApiMappingDefinition,
  MappingKey as OpenApiMappingKey,
  Trait as OpenApiTrait
}
import it.agilelab.dataplatformshaper.uservice.{Handler, Resource}

import java.io.{ByteArrayInputStream, FileReader}
import scala.Tuple.*
import scala.language.implicitConversions
import scala.util.Try

class OntologyManagerHandler[F[_]: Async](
  tms: TypeManagementServiceInterpreter[F],
  ims: InstanceManagementServiceInterpreter[F],
  trms: TraitManagementServiceInterpreter[F],
  mms: MappingManagementService[F]
) extends Handler[F]
    with StrictLogging:

  override def createType(respond: Resource.CreateTypeResponse.type)(
    body: OpenApiEntityType
  ): F[CreateTypeResponse] =
    val schema: Schema = body.schema
    val fatherName = body.fatherName
    val traits =
      body.traits.fold(Set.empty[String])(x => x.map(str => str).toSet)
    val res = (for {
      _ <- EitherT(
        fatherName
          .fold(
            tms
              .create(EntityType(body.name, traits, schema, None))
          )(fn =>
            tms
              .create(EntityType(body.name, traits, schema, None), fn)
          )
          .map {
            _.leftMap { case ManagementServiceError(errors) =>
              errors.toVector
            }
          }
      )
    } yield ()).value

    res
      .map {
        case Left(errors) => respond.BadRequest(ValidationError(errors))
        case Right(_)     => respond.Ok("OK")
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end createType

  private def processBulkTypeRequest(
    openApiRequest: OpenApiBulkEntityTypesCreationRequest
  ): F[OpenApiBulkEntityTypesCreationResponse] =
    openApiRequest.entityTypes
      .map(tp =>
        tp.fatherName
          .fold(
            tms
              .create(
                EntityType(
                  tp.name,
                  tp.traits.fold(Set.empty[String])(_.toSet),
                  tp.schema: Schema,
                  None
                )
              )
              .map(et => (tp, et.fold(_.errors.mkString(","), _ => "OK")))
          )(fn =>
            tms
              .create(
                EntityType(
                  tp.name,
                  tp.traits.fold(Set.empty[String])(_.toSet),
                  tp.schema: Schema,
                  None
                ),
                fn
              )
              .map(et => (tp, et.fold(_.errors.mkString(","), _ => "OK")))
          )
      )
      .sequence
      .map(
        _.map(p =>
          OpenApiBulkEntityTypesCreationResponse.EntityTypes(p(0), p(1))
        )
      )
      .map(OpenApiBulkEntityTypesCreationResponse.apply)
  end processBulkTypeRequest

  override def createTypeBulk(respond: Resource.CreateTypeBulkResponse.type)(
    body: OpenApiBulkEntityTypesCreationRequest
  ): F[CreateTypeBulkResponse] =
    processBulkTypeRequest(body).map(res => respond.Ok(res))
  end createTypeBulk

  override def createTypeBulkByYaml(
    respond: Resource.CreateTypeBulkByYamlResponse.type
  )(body: Stream[F, Byte]): F[CreateTypeBulkByYamlResponse] =
    val eitherRequest
      : F[Either[String, OpenApiBulkEntityTypesCreationRequest]] =
      body
        .through(text.utf8.decode)
        .fold("")(_ + _)
        .compile
        .toList
        .map(_.head)
        .map(parser.parse(_).leftMap(_.getMessage))
        .map(
          _.flatMap(json =>
            OpenApiBulkEntityTypesCreationRequest
              .decodeBulkEntityTypesCreationRequest(json.hcursor)
              .leftMap(_.getMessage)
          )
        )
    eitherRequest.flatMap {
      case Left(error) =>
        respond.BadRequest(ValidationError(Vector(error))).pure[F]
      case Right(openApiRequest) =>
        processBulkTypeRequest(openApiRequest).map(res => respond.Ok(res))
    }
  end createTypeBulkByYaml

  override def deleteType(respond: Resource.DeleteTypeResponse.type)(
    name: String
  ): F[Resource.DeleteTypeResponse] =
    val res: F[Either[String, String]] = for {
      deleteResult <- tms.delete(name).map(_.bimap(_.errors.head, _ => "OK"))
    } yield deleteResult

    res
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(successMessage) => respond.Ok(successMessage)
      }
      .onError { t =>
        logger.error(s"Error: ${t.getMessage}").pure[F]
      }
  end deleteType

  override def updateTypeConstraints(
    respond: Resource.UpdateTypeConstraintsResponse.type
  )(body: OpenApiEntityType): F[UpdateTypeConstraintsResponse] =

    val schema: Schema = body.schema
    val entityType: EntityType =
      EntityType(body.name, Set(), schema, None)

    val res = for {
      res <- EitherT(
        tms
          .updateConstraints(entityType)
          .map {
            _.leftMap { case ManagementServiceError(errors) =>
              errors.toVector
            }
          }
      )
    } yield res

    res.value
      .map {
        case Left(errors) => respond.BadRequest(ValidationError(errors))
        case Right(_)     => respond.Ok("OK")
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end updateTypeConstraints

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
              .create(EntityType(entityType.name, ts, entityType.schema, None))
          )(fn =>
            tms
              .create(
                EntityType(entityType.name, ts, entityType.schema, None),
                fn
              )
          )
          .map(_.leftMap(_.errors.head))
      )
    } yield res

    res.value
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(_)    => respond.Ok("OK")
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end createTypeByYaml

  override def updateTypeConstraintsByYaml(
    respond: Resource.UpdateTypeConstraintsByYamlResponse.type
  )(body: Stream[F, Byte]): F[Resource.UpdateTypeConstraintsByYamlResponse] =
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
      ts = entityType.traits.fold(Set.empty[String])(_.map(identity).toSet)
      res <- EitherT(
        tms
          .updateConstraints(
            EntityType(entityType.name, ts, entityType.schema, None)
          )
          .map(_.leftMap(_.errors.head))
      )
    } yield res

    res.value
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(_)    => respond.Ok("OK")
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end updateTypeConstraintsByYaml

  override def readType(respond: Resource.ReadTypeResponse.type)(
    name: String
  ): F[Resource.ReadTypeResponse] =

    val res = for {
      et <- tms
        .read(name)
        .map(_.leftMap(_.errors.head))
    } yield et

    res
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(entityType) => respond.Ok(entityType)
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end readType

  override def readTypeAsYaml(respond: Resource.ReadTypeAsYamlResponse.type)(
    name: String
  ): F[Resource.ReadTypeAsYamlResponse[F]] =
    val res = (for {
      et <- EitherT(
        tms
          .read(name)
          .map(_.leftMap(_.errors.head))
      )
      stream <- EitherT(
        Right(
          readInputStream(
            ByteArrayInputStream(
              OpenApiEntityType
                .encodeEntityType(et)
                .asYaml
                .spaces2
                .getBytes("UTF8")
            ).pure[F],
            128
          )
        ).pure[F]
      )
    } yield stream).value

    res
      .map {
        case Left(error)   => respond.BadRequest(ValidationError(Vector(error)))
        case Right(stream) => respond.Ok(stream)
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end readTypeAsYaml

  override def createEntity(respond: Resource.CreateEntityResponse.type)(
    body: OpenApiEntity
  ): F[Resource.CreateEntityResponse] =
    val res = (for {
      schema <- EitherT(
        tms
          .read(body.entityTypeName)
          .map(_.map(_.schema))
          .map(_.leftMap(l => Vector(l.errors.head)))
      )
      tuple <- EitherT(
        jsonToTuple(body.values, schema)
          .leftMap(l => Vector(l.getMessage))
          .pure[F]
      )
      entityId <- EitherT(
        ims
          .create(body.entityTypeName, tuple)
          .map(_.leftMap { case ManagementServiceError(errors) =>
            errors.toVector
          })
      )
    } yield entityId).value
    res
      .map {
        case Left(errors)    => respond.BadRequest(ValidationError(errors))
        case Right(entityId) => respond.Ok(entityId)
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end createEntity

  override def deleteEntity(
    respond: Resource.DeleteEntityResponse.type
  )(deleteId: String): F[Resource.DeleteEntityResponse] =
    ims
      .delete(deleteId)
      .map {
        case Left(error) =>
          respond.BadRequest(ValidationError(Vector(error.errors.head)))
        case Right(_) => respond.Ok("Entity deleted successfully")
      }
      .onError { t =>
        logger.error(s"Error: ${t.getMessage}").pure[F]
      }
  end deleteEntity

  override def updateEntity(
    respond: Resource.UpdateEntityResponse.type
  )(updateId: String, body: OpenApiEntity): F[Resource.UpdateEntityResponse] =
    val res = (for {
      schema <- EitherT(
        tms
          .read(body.entityTypeName)
          .map(_.map(_.schema))
          .map(_.leftMap(l => Vector(l.errors.head)))
      )
      tuple <- EitherT(
        jsonToTuple(body.values, schema)
          .leftMap(l => Vector(l.getMessage))
          .pure[F]
      )
      entityId <- EitherT(
        ims
          .update(updateId, tuple)
          .map(_.leftMap { case ManagementServiceError(errors) =>
            errors.toVector
          })
      )
    } yield entityId).value

    res
      .map {
        case Left(errors)    => respond.BadRequest(ValidationError(errors))
        case Right(entityId) => respond.Ok(entityId)
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
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
          .map(_.leftMap(_.errors.head))
      )
      tuple <- EitherT(
        jsonToTuple(body.values, schema).leftMap(_.getMessage).pure[F]
      )
      _ <- EitherT(ims.update(id, tuple).map(_.leftMap(_.errors.head)))
    } yield id).value

    res
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(updatedId) => respond.Ok(updatedId)
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}}").pure[F])
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
          .map(_.leftMap(_.errors.head))
      )
      tuple <- EitherT(
        jsonToTuple(body.values, schema).leftMap(_.getMessage).pure[F]
      )
      entityId <- EitherT(
        ims.create(body.entityTypeName, tuple).map(_.leftMap(_.errors.head))
      )
    } yield entityId).value

    res
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(entityId) => respond.Ok(entityId)
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end createEntityByYaml

  override def readEntity(respond: Resource.ReadEntityResponse.type)(
    id: String
  ): F[Resource.ReadEntityResponse] =
    val res = (for {
      et <- EitherT(ims.read(id).map(_.leftMap(_.errors.head)))
      schema <- EitherT(
        tms.read(et.entityTypeName).map(_.map(_.schema).leftMap(_.errors.head))
      )
      values <- EitherT(
        tupleToJson(et.values, schema).leftMap(_.getMessage).pure[F]
      )
      oaEntity <- EitherT(
        Right(OpenApiEntity(et.entityId, et.entityTypeName, values)).pure[F]
      )
    } yield oaEntity).value

    res
      .map {
        case Left(error) => respond.BadRequest(ValidationError(Vector(error)))
        case Right(oaEntity) => respond.Ok(oaEntity)
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end readEntity

  override def readEntityAsYaml(
    respond: Resource.ReadEntityAsYamlResponse.type
  )(id: String): F[Resource.ReadEntityAsYamlResponse[F]] =
    val res = (for {
      et <- EitherT(ims.read(id).map(_.leftMap(_.errors.head)))
      schema <- EitherT(
        tms.read(et.entityTypeName).map(_.map(_.schema).leftMap(_.errors.head))
      )
      values <- EitherT(
        tupleToJson(et.values, schema).leftMap(_.getMessage).pure[F]
      )
      oaEntity <- EitherT(
        Right(
          readInputStream(
            ByteArrayInputStream(
              OpenApiEntity
                .encodeEntity(
                  OpenApiEntity(et.entityId, et.entityTypeName, values)
                )
                .asYaml
                .spaces2
                .getBytes("UTF8")
            ).pure[F],
            128
          )
        ).pure[F]
      )
    } yield oaEntity).value

    res
      .map {
        case Left(error)   => respond.BadRequest(ValidationError(Vector(error)))
        case Right(stream) => respond.Ok(stream)
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end readEntityAsYaml

  override def createTrait(respond: Resource.CreateTraitResponse.type)(
    body: OpenApiTrait
  ): F[Resource.CreateTraitResponse] =
    val res = trms.create(Trait(body.name, body.inheritsFrom))

    res.map {
      case Left(error) =>
        logger.error(s"Error: ${error.errors.head}")
        respond.BadRequest(ValidationError(Vector(error.errors.head)))
      case Right(_) =>
        respond.Ok("Trait created successfully")
    }
  end createTrait

  override def createTraitBulk(respond: Resource.CreateTraitBulkResponse.type)(
    body: OpenApiBulkTraitsCreationRequest
  ): F[CreateTraitBulkResponse] =
    val request = BulkTraitsCreationRequest(
      body.traits.map(tr => Trait(tr.name, tr.inheritsFrom)).toList,
      body.relationships
        .map(re => (re.subject, re.relationship: Relationship, re.`object`))
        .toList
    )
    trms
      .create(request)
      .map(res =>
        OpenApiBulkTraitsCreationResponse(
          res._1
            .map(p =>
              Traits(
                OpenApiTrait(p._1.traitName, p._1.inheritsFrom),
                p._2.getOrElse("OK")
              )
            )
            .toVector,
          res._2
            .map(t =>
              Relationships(
                First(t._1._1, t._1._2: String, t._1._3),
                t._2.getOrElse("OK")
              )
            )
            .toVector
        )
      )
      .map(res => respond.Ok(res))
  end createTraitBulk

  override def createTraitBulkByYaml(
    respond: Resource.CreateTraitBulkByYamlResponse.type
  )(body: Stream[F, Byte]): F[CreateTraitBulkByYamlResponse] =
    val eitherRequest: F[Either[String, OpenApiBulkTraitsCreationRequest]] =
      body
        .through(text.utf8.decode)
        .fold("")(_ + _)
        .compile
        .toList
        .map(_.head)
        .map(parser.parse(_).leftMap(_.getMessage))
        .map(
          _.flatMap(json =>
            OpenApiBulkTraitsCreationRequest
              .decodeBulkTraitsCreationRequest(json.hcursor)
              .leftMap(_.getMessage)
          )
        )

    eitherRequest.flatMap {
      case Left(error) =>
        respond.BadRequest(ValidationError(Vector(error))).pure[F]
      case Right(openApiRequest) =>
        val request = BulkTraitsCreationRequest(
          openApiRequest.traits
            .map(tr => Trait(tr.name, tr.inheritsFrom))
            .toList,
          openApiRequest.relationships
            .map(re => (re.subject, re.relationship: Relationship, re.`object`))
            .toList
        )
        trms
          .create(request)
          .map(res =>
            OpenApiBulkTraitsCreationResponse(
              res._1
                .map(p =>
                  Traits(
                    OpenApiTrait(p._1.traitName, p._1.inheritsFrom),
                    p._2.getOrElse("OK")
                  )
                )
                .toVector,
              res._2
                .map(t =>
                  Relationships(
                    First(t._1._1, t._1._2: String, t._1._3),
                    t._2.getOrElse("OK")
                  )
                )
                .toVector
            )
          )
          .map(res => respond.Ok(res))
    }
  end createTraitBulkByYaml

  override def deleteTrait(respond: Resource.DeleteTraitResponse.type)(
    traitName: String
  ): F[Resource.DeleteTraitResponse] =
    val res = trms.delete(traitName)

    res.map {
      case Left(error) =>
        logger.error(s"Error: ${error.errors.head}")
        respond.BadRequest(ValidationError(Vector(error.errors.head)))
      case Right(_) =>
        respond.Ok("Trait deleted successfully")
    }
  end deleteTrait

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
              s"Service error in linking traits: ${serviceError.errors.head}"
            )
            respond.BadRequest(
              ValidationError(Vector(serviceError.errors.head))
            )
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
              s"Service error in unlinking traits: ${serviceError.errors.head}"
            )
            respond.BadRequest(
              ValidationError(Vector(serviceError.errors.head))
            )
          case Right(_) =>
            respond.Ok(
              s"Traits $trait1 and $trait2 with relationship $rel unlinked successfully"
            )
        end match
    }
  end unlinkTrait

  override def linkedTraits(
    respond: Resource.LinkedTraitsResponse.type
  )(traitName: String, rel: String): F[Resource.LinkedTraitsResponse] =

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
              s"Service error in getting linked traits: ${serviceError.errors.head}"
            )
            respond.BadRequest(
              ValidationError(Vector(serviceError.errors.head))
            )
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
              s"Service error in linking traits: ${serviceError.errors.head}"
            )
            respond.BadRequest(
              ValidationError(Vector(serviceError.errors.head))
            )
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
              s"Service error in getting linked traits: ${serviceError.errors.head}"
            )
            respond.BadRequest(
              ValidationError(Vector(serviceError.errors.head))
            )
          case Right(_) =>
            respond.Ok(
              s"Instances with ids $instanceId1 and $instanceId2 and with relationship $rel unlinked successfully"
            )
        end match
    }
  end unlinkEntity

  override def linkedEntities(
    respond: Resource.LinkedEntitiesResponse.type
  )(instanceId: String, rel: String): F[Resource.LinkedEntitiesResponse] =

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
              s"Service error in getting linked entities: ${serviceError.errors.head}"
            )
            respond.BadRequest(
              ValidationError(Vector(serviceError.errors.head))
            )
          case Right(entitiesList) =>
            respond.Ok(entitiesList.toVector)
        }
    }
  end linkedEntities

  @SuppressWarnings(Array("scalafix:DisableSyntax.throw"))
  override def listEntities(respond: Resource.ListEntitiesResponse.type)(
    entityTypeName: String,
    query: String,
    limit: Option[Int]
  ): F[Resource.ListEntitiesResponse] =
    (for {
      schema <- EitherT(tms.read(entityTypeName).map(_.map(_.schema)))
      listEntities <- EitherT(ims.list(entityTypeName, query, true, limit))
    } yield (schema, listEntities)).value
      .map(
        _.map(p =>
          p(1).toVector.map({
            case et: Entity =>
              tupleToJson(et.values, p(0)) match
                case Left(error) =>
                  logger.error(
                    s"Error querying instances with type $entityTypeName and query $query: ${error.getMessage}"
                  )
                  throw Exception("It shouldn't be here")
                case Right(json) =>
                  OpenApiEntity(et.entityId, et.entityTypeName, json)
              end match
            case _: String => throw Exception("It shouldn't be here")
          })
        )
      )
      .map({
        case Left(error) =>
          respond.BadRequest(ValidationError(Vector(error.errors.head)))
        case Right(entities) =>
          respond.Ok(entities)
      })
  end listEntities

  override def listTypes(
    respond: Resource.ListTypesResponse.type
  )(): F[Resource.ListTypesResponse] =
    val result = for
      entityTypes <- EitherT(tms.list())
      transformedTypes = entityTypes.map { entityType =>
        val traitOptions =
          if entityType.traits.nonEmpty then Some(entityType.traits.toVector)
          else None
        OpenApiEntityType(
          entityType.name,
          traitOptions,
          entityType.schema,
          entityType.fatherName
        )
      }
    yield transformedTypes.toVector

    result.value.flatMap {
      case Right(entityTypes) =>
        respond.Ok(entityTypes).pure[F]
      case Left(error) =>
        respond.BadRequest(ValidationError(error.errors.toVector)).pure[F]
    }
  end listTypes

  override def listTraits(
    respond: Resource.ListTraitsResponse.type
  )(): F[Resource.ListTraitsResponse] =
    val result =
      for traitNames <- EitherT(trms.list())
      yield traitNames.toVector

    result.value.flatMap {
      case Right(traitNames) =>
        respond.Ok(traitNames).pure[F]
      case Left(error) =>
        respond.BadRequest(ValidationError(error.errors.toVector)).pure[F]
    }
  end listTraits

  @SuppressWarnings(Array("scalafix:DisableSyntax.throw"))
  override def listEntitiesByIds(
    respond: Resource.ListEntitiesByIdsResponse.type
  )(
    entityTypeName: String,
    query: String,
    limit: Option[Int]
  ): F[Resource.ListEntitiesByIdsResponse] =
    ims
      .list(entityTypeName, query, false, limit)
      .map({
        case Left(error) =>
          logger.error(
            s"Error querying instances with type $entityTypeName and query $query: ${error.errors.head}"
          )
          respond.BadRequest(ValidationError(Vector(error.errors.head)))
        case Right(entities) =>
          respond.Ok(entities.toVector.map {
            case str: String => str
            case _           => throw new Exception("Unexpected entity type")
          }: Vector[String])
      })

  override def readMapping(respond: Resource.ReadMappingResponse.type)(
    mappingName: String,
    sourceTypeName: String,
    targetTypeName: String
  ): F[Resource.ReadMappingResponse] =
    (for {
      mappingDefinition <- EitherT(
        mms.read(MappingKey(mappingName, sourceTypeName, targetTypeName))
      )
      mapperSchema <- EitherT(
        tms
          .read(mappingDefinition.mappingKey.targetEntityTypeName)
          .map(_.map(t => schemaToMapperSchema(t.schema)))
      )
      res <- EitherT(
        tupleToJson(mappingDefinition.mapper, mapperSchema)
          .leftMap(e => ManagementServiceError(e.getMessage))
          .map(
            OpenApiMappingDefinition(
              OpenApiMappingKey(
                mappingDefinition.mappingKey.mappingName,
                mappingDefinition.mappingKey.sourceEntityTypeName,
                mappingDefinition.mappingKey.targetEntityTypeName
              ),
              _,
              mappingDefinition.additionalSourcesReferences
            )
          )
          .pure[F]
      )
    } yield res).value.map {
      case Left(error) =>
        logger.error(s"Error in retrieving the mapping: ${error.errors.head}")
        respond.BadRequest(ValidationError(error.errors.toVector))
      case Right(md) =>
        respond.Ok(md)
    }
  end readMapping

  override def readMappingAsYaml(
    respond: Resource.ReadMappingAsYamlResponse.type
  )(
    mappingName: String,
    sourceTypeName: String,
    targetTypeName: String
  ): F[Resource.ReadMappingAsYamlResponse[F]] =
    (for {
      mappingDefinition <- EitherT(
        mms.read(MappingKey(mappingName, sourceTypeName, targetTypeName))
      )
      mapperSchema <- EitherT(
        tms
          .read(mappingDefinition.mappingKey.targetEntityTypeName)
          .map(_.map(t => schemaToMapperSchema(t.schema)))
      )
      openApiMappingDefinition <- EitherT(
        tupleToJson(mappingDefinition.mapper, mapperSchema)
          .leftMap(e => ManagementServiceError(e.getMessage))
          .map(
            OpenApiMappingDefinition(
              OpenApiMappingKey(
                mappingDefinition.mappingKey.mappingName,
                mappingDefinition.mappingKey.sourceEntityTypeName,
                mappingDefinition.mappingKey.targetEntityTypeName
              ),
              _,
              mappingDefinition.additionalSourcesReferences
            )
          )
          .pure[F]
      )
      res <- EitherT(
        Right(
          readInputStream(
            ByteArrayInputStream(
              OpenApiMappingDefinition
                .encodeMappingDefinition(openApiMappingDefinition)
                .asYaml
                .spaces2
                .getBytes("UTF8")
            ).pure[F],
            128
          )
        ).pure[F]
      )
    } yield res).value map {
      case Left(error) =>
        logger.error(s"Error in retrieving the mapping: ${error.errors.head}")
        respond.BadRequest(ValidationError(error.errors.toVector))
      case Right(stream) =>
        respond.Ok(stream)
    }
  end readMappingAsYaml

  override def deleteMapping(respond: Resource.DeleteMappingResponse.type)(
    mappingName: String,
    sourceTypeName: String,
    targetTypeName: String
  ): F[Resource.DeleteMappingResponse] =
    val res =
      mms.delete(MappingKey(mappingName, sourceTypeName, targetTypeName))
    res
      .map {
        case Left(error) =>
          respond.BadRequest(ValidationError(error.errors.toVector))
        case Right(()) => respond.Ok("Mapping deleted successfully")
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end deleteMapping

  override def updateMapping(respond: Resource.UpdateMappingResponse.type)(
    body: OpenApiMappingDefinition
  ): F[Resource.UpdateMappingResponse] =
    val res = (for {
      schema <- EitherT(
        tms
          .read(body.mappingKey.targetEntityTypeName)
          .map(_.map(_.schema))
          .map(_.leftMap(l => Vector(l.errors.head)))
      )
      tuple <- EitherT(
        jsonToTuple(body.mapper, schemaToMapperSchema(schema))
          .leftMap(l => Vector(l.getMessage))
          .pure[F]
      )
      _ <- EitherT.liftF(
        mms.update(
          MappingKey(
            body.mappingKey.mappingName,
            body.mappingKey.sourceEntityTypeName,
            body.mappingKey.targetEntityTypeName
          ),
          tuple
        )
      )
    } yield ()).value
    res
      .map {
        case Left(errors) => respond.BadRequest(ValidationError(errors))
        case Right(())    => respond.Ok("Mapping updated successfully")
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end updateMapping

  override def createMapping(respond: Resource.CreateMappingResponse.type)(
    body: OpenApiMappingDefinition
  ): F[Resource.CreateMappingResponse] =
    val res = (for {
      schema <- EitherT(
        tms
          .read(body.mappingKey.targetEntityTypeName)
          .map(_.map(_.schema))
          .map(_.leftMap(l => Vector(l.errors.head)))
      )
      tuple <- EitherT(
        jsonToTuple(body.mapper, schemaToMapperSchema(schema))
          .leftMap(l => Vector(l.getMessage))
          .pure[F]
      )
      _ <- EitherT(
        mms
          .create(
            MappingDefinition(
              MappingKey(
                body.mappingKey.mappingName,
                body.mappingKey.sourceEntityTypeName,
                body.mappingKey.targetEntityTypeName
              ),
              tuple,
              body.additionalSourcesReferences
            )
          )
          .map(_.leftMap { case err: ManagementServiceError =>
            Vector(err.errors.head)
          })
      )
    } yield ()).value
    res
      .map {
        case Left(errors) => respond.BadRequest(ValidationError(errors))
        case Right(())    => respond.Ok("Mapping created successfully")
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end createMapping

  override def createMappingBulk(
    respond: Resource.CreateMappingBulkResponse.type
  )(body: OpenApiBulkMappingsCreationRequest): F[CreateMappingBulkResponse] =
    val createMapRes = bulkMapCreator(body)
    createMapRes
      .map(OpenApiBulkMappingsCreationResponse.apply)
      .map(res => respond.Ok(res))
  end createMappingBulk

  override def createMappingBulkByYaml(
    respond: Resource.CreateMappingBulkByYamlResponse.type
  )(body: Stream[F, Byte]): F[CreateMappingBulkByYamlResponse] =
    val eitherRequest: F[Either[String, OpenApiBulkMappingsCreationRequest]] =
      body
        .through(text.utf8.decode)
        .fold("")(_ + _)
        .compile
        .toList
        .map(_.head)
        .map(parser.parse(_).leftMap(_.getMessage))
        .map(
          _.flatMap(json =>
            OpenApiBulkMappingsCreationRequest
              .decodeBulkMappingsCreationRequest(json.hcursor)
              .leftMap(_.getMessage)
          )
        )

    eitherRequest.flatMap {
      case Left(error) =>
        respond.BadRequest(ValidationError(Vector(error))).pure[F]
      case Right(openApiRequest) =>
        val createMapRes = bulkMapCreator(openApiRequest)
        createMapRes
          .map(OpenApiBulkMappingsCreationResponse.apply)
          .map(res => respond.Ok(res))
    }
  end createMappingBulkByYaml

  private def bulkMapCreator(
    openApiRequest: OpenApiBulkMappingsCreationRequest
  ): F[Vector[OpenApiBulkMappingsCreationResponse.MappingDefinitions]] =
    openApiRequest.mappingDefinitions
      .map(md =>
        val schemaEither = tms
          .read(md.mappingKey.targetEntityTypeName)
          .map(_.map(_.schema))
          .map(_.leftMap(l => Vector(l.errors.head)))
        val tupleEither =
          schemaEither.map(
            _.map(schema =>
              jsonToTuple(md.mapper, schemaToMapperSchema(schema))
            )
          )
        val tupleMappedEither: F[Either[Vector[String], Tuple]] =
          tupleEither
            .map {
              case Left(errors) => Left(errors)
              case Right(parsedResult) =>
                parsedResult
                  .leftMap(parsingFailure => Vector(parsingFailure.getMessage))
            }
            .map {
              case Left(errors) => Left(errors)
              case Right(tuple) => Right(tuple)
            }
        val mappedResult = tupleMappedEither
          .map(
            _.map(tuple =>
              mms
                .create(
                  MappingDefinition(
                    MappingKey(
                      md.mappingKey.mappingName,
                      md.mappingKey.sourceEntityTypeName,
                      md.mappingKey.targetEntityTypeName
                    ),
                    tuple,
                    md.additionalSourcesReferences
                  )
                )
                .map(
                  _.fold(err => (md, err.errors.mkString(",")), _ => (md, "OK"))
                )
            )
          )
          .map(_.fold(errors => (md, errors.mkString(",")).pure[F], identity))
          .flatten
        mappedResult.map(m =>
          OpenApiBulkMappingsCreationResponse.MappingDefinitions(m._1, m._2)
        )
      )
      .sequence
  end bulkMapCreator

  override def createMappingByYaml(
    respond: Resource.CreateMappingByYamlResponse.type
  )(body: Stream[F, Byte]): F[Resource.CreateMappingByYamlResponse] =
    val getMappingDefinition = body
      .through(text.utf8.decode)
      .fold("")(_ + _)
      .compile
      .toList
      .map(_.head)
      .map(parser.parse(_).leftMap(_.getMessage))
      .map(
        _.flatMap(json =>
          OpenApiMappingDefinition
            .decodeMappingDefinition(json.hcursor)
            .leftMap(_.getMessage)
        ).leftMap(err => Vector(err))
      )

    val res = (for {
      body <- EitherT(getMappingDefinition)
      schema <- EitherT(
        tms
          .read(body.mappingKey.targetEntityTypeName)
          .map(_.map(_.schema))
          .map(_.leftMap(l => Vector(l.errors.head)))
      )
      tuple <- EitherT(
        jsonToTuple(body.mapper, schemaToMapperSchema(schema))
          .leftMap(l => Vector(l.getMessage))
          .pure[F]
      )
      _ <- EitherT(
        mms
          .create(
            MappingDefinition(
              MappingKey(
                body.mappingKey.mappingName,
                body.mappingKey.sourceEntityTypeName,
                body.mappingKey.targetEntityTypeName
              ),
              tuple,
              body.additionalSourcesReferences
            )
          )
          .map(_.leftMap(err => Vector(err.errors.head)))
      )
    } yield ()).value
    res
      .map {
        case Left(errors) => respond.BadRequest(ValidationError(errors))
        case Right(())    => respond.Ok("Mapping created successfully")
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end createMappingByYaml

  override def createMappedInstances(
    respond: Resource.CreateMappedInstancesResponse.type
  )(body: String): F[Resource.CreateMappedInstancesResponse] =
    val res = mms
      .createMappedInstances(body)
      .map(_.leftMap(err => Vector(err.errors.head)))
    res
      .map {
        case Left(errors) => respond.BadRequest(ValidationError(errors))
        case Right(())    => respond.Ok("Mapped instances created successfully")
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end createMappedInstances

  override def updateMappedInstances(
    respond: Resource.UpdateMappedInstancesResponse.type
  )(body: String): F[Resource.UpdateMappedInstancesResponse] =
    val res = mms
      .updateMappedInstances(body)
      .map(_.leftMap(err => Vector(err.errors.head)))
    res
      .map {
        case Left(errors) => respond.BadRequest(ValidationError(errors))
        case Right(())    => respond.Ok("Mapping created successfully")
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end updateMappedInstances

  override def deleteMappedInstances(
    respond: Resource.DeleteMappedInstancesResponse.type
  )(sourceInstanceId: String): F[Resource.DeleteMappedInstancesResponse] =
    val res = mms
      .deleteMappedInstances(sourceInstanceId)
      .map(_.leftMap(err => Vector(err.errors.head)))
    res
      .map {
        case Left(errors) => respond.BadRequest(ValidationError(errors))
        case Right(())    => respond.Ok("Mapping deleted successfully")
      }
      .onError(t => logger.error(s"Error: ${t.getMessage}").pure[F])
  end deleteMappedInstances

  @SuppressWarnings(Array("scalafix:DisableSyntax.throw"))
  override def readMappedInstances(
    respond: Resource.ReadMappedInstancesResponse.type
  )(sourceInstanceId: String): F[Resource.ReadMappedInstancesResponse] =
    (for {
      listItems <- EitherT(mms.readMappedInstances(sourceInstanceId))
    } yield listItems).value
      .map(_.map(_.toVector.map({
        case (
              (sourceEntityType, sourceEntity),
              mappingRelationship,
              (targetEntityType, targetEntity)
            ) =>
          val se =
            tupleToJson(sourceEntity.values, sourceEntityType.schema) match
              case Left(error) =>
                logger.error(
                  s"Error getting mapped instances: ${error.getMessage}"
                )
                throw Exception("It shouldn't be here")
              case Right(json) =>
                OpenApiEntity(
                  sourceEntity.entityId,
                  sourceEntity.entityTypeName,
                  json
                )
            end match
          val te =
            tupleToJson(targetEntity.values, targetEntityType.schema) match
              case Left(error) =>
                logger.error(
                  s"Error getting mapped instances: ${error.getMessage}"
                )
                throw Exception("It shouldn't be here")
              case Right(json) =>
                OpenApiEntity(
                  targetEntity.entityId,
                  targetEntity.entityTypeName,
                  json
                )
            end match
          MappedInstancesItem(se, mappingRelationship, te)
      })))
      .map({
        case Left(error) =>
          respond.BadRequest(ValidationError(Vector(error.errors.head)))
        case Right(entities) =>
          respond.Ok(entities)
      })
  end readMappedInstances

  @SuppressWarnings(Array("scalafix:DisableSyntax.throw"))
  private def processBulkCreation(jsons: List[Json]): F[Stream[F, Byte]] =
    val streams = jsons.map { json =>
      val names = json.hcursor.keys
        .getOrElse(throw new Exception("Not found any keys"))
        .toList
      names match {
        case List("traits", "relationships") =>
          val processTraitsResult: F[Either[String, Stream[F, Byte]]] = (for {
            traitsEither <- EitherT.fromEither(
              OpenApiBulkTraitsCreationRequest
                .decodeBulkTraitsCreationRequest(json.hcursor)
                .leftMap(_.getMessage)
            )
            bulkRequest = BulkTraitsCreationRequest(
              traitsEither.traits
                .map(tr => Trait(tr.name, tr.inheritsFrom))
                .toList,
              traitsEither.relationships
                .map(re =>
                  (re.subject, re.relationship: Relationship, re.`object`)
                )
                .toList
            )
            bulkTraitsResponse <- EitherT.liftF(
              trms
                .create(bulkRequest)
            )
            streamResult <- EitherT(
              Right(
                readInputStream(
                  ByteArrayInputStream(
                    OpenApiBulkTraitsCreationResponse
                      .encodeBulkTraitsCreationResponse(
                        OpenApiBulkTraitsCreationResponse(
                          bulkTraitsResponse.traits
                            .map(t =>
                              Traits(
                                OpenApiTrait(t._1.traitName, t._1.inheritsFrom),
                                t._2.getOrElse("OK")
                              )
                            )
                            .toVector,
                          bulkTraitsResponse.relationships
                            .map(r =>
                              OpenApiBulkTraitsCreationResponse
                                .Relationships(
                                  First(r._1._1, r._1._2: String, r._1._3),
                                  r._2.getOrElse("OK")
                                )
                            )
                            .toVector
                        )
                      )
                      .asYaml
                      .spaces2
                      .getBytes("UTF8")
                  ).pure[F],
                  128
                )
              ).pure[F]
            )
          } yield streamResult).value
          processTraitsResult
        case List("entityTypes") =>
          val streamTraitsResult: F[Either[String, Stream[F, Byte]]] = (for {
            bulkTypeRequest <- EitherT.fromEither(
              OpenApiBulkEntityTypesCreationRequest
                .decodeBulkEntityTypesCreationRequest(json.hcursor)
                .leftMap(_.getMessage)
            )
            bulkTypeResponse <- EitherT.liftF(
              processBulkTypeRequest(bulkTypeRequest)
            )
            streamResult <- EitherT(
              Right(
                readInputStream(
                  ByteArrayInputStream(
                    OpenApiBulkEntityTypesCreationResponse
                      .encodeBulkEntityTypesCreationResponse(bulkTypeResponse)
                      .asYaml
                      .spaces2
                      .getBytes("UTF8")
                  ).pure[F],
                  128
                )
              ).pure[F]
            )
          } yield streamResult).value
          streamTraitsResult
        case List("mappingDefinitions") =>
          val streamMappingDefinitionResult
            : F[Either[String, Stream[F, Byte]]] = (for {
            bulkMappingDefinitionRequest <- EitherT.fromEither(
              OpenApiBulkMappingsCreationRequest
                .decodeBulkMappingsCreationRequest(json.hcursor)
                .leftMap(_.getMessage)
            )
            mappingDefinitionVector <- EitherT.liftF(
              bulkMapCreator(bulkMappingDefinitionRequest)
            )
            bulkMappingDefinitionResponse = OpenApiBulkMappingsCreationResponse(
              mappingDefinitionVector
            )
            streamResult <- EitherT(
              Right(
                readInputStream(
                  ByteArrayInputStream(
                    OpenApiBulkMappingsCreationResponse
                      .encodeBulkMappingsCreationResponse(
                        bulkMappingDefinitionResponse
                      )
                      .asYaml
                      .spaces2
                      .getBytes("UTF8")
                  ).pure[F],
                  128
                )
              ).pure[F]
            )
          } yield streamResult).value
          streamMappingDefinitionResult
        case _ =>
          val streamUnknown: F[Either[String, Stream[F, Byte]]] =
            Right(
              Stream
                .eval(
                  (
                    Stream.emits("Unknown file".getBytes) ++ Stream
                      .emits(json.noSpaces.getBytes)
                  ).pure[F]
                )
                .flatten
            ).pure[F]
          streamUnknown
      }
    }
    val streamsList = streams.map(_.map {
      case Left(s) =>
        Stream.eval(s.getBytes.toSeq.pure[F]).flatMap(Stream.emits)
      case Right(s) => s
    })
    val separator: Stream[F, Byte] = Stream.emits("---\n".getBytes)
    streamsList.sequence.map(_.reduce(_ ++ separator ++ _))
  end processBulkCreation

  override def bulkCreationYaml(
    respond: Resource.BulkCreationYamlResponse.type
  )(body: Stream[F, Byte]): F[BulkCreationYamlResponse[F]] =
    val yamlStringReader: F[Either[Throwable, String]] =
      body.through(fs2.text.utf8.decode).compile.foldMonoid.attempt
    val res: F[BulkCreationYamlResponse[F]] = yamlStringReader.flatMap {
      case Left(ex) =>
        BulkCreationYamlResponse
          .BadRequest(ValidationError(Vector(ex.toString)))
          .pure[F]
      case Right(yamlContent) =>
        val yamlFileReader: Either[Throwable, FileReader] =
          Try {
            val tempFile = java.io.File.createTempFile("yamlContent", ".yaml")
            val writer = java.io.PrintWriter(tempFile)
            writer.write(yamlContent)
            writer.close()
            FileReader(tempFile.getAbsolutePath)
          }.toEither
        yamlFileReader match
          case Left(ex) =>
            BulkCreationYamlResponse
              .BadRequest(ValidationError(Vector(ex.toString)))
              .pure[F]
          case Right(reader) =>
            val allFiles = yaml.parser.parseDocuments(reader).toList.sequence
            allFiles match
              case Left(parseError) =>
                BulkCreationYamlResponse
                  .BadRequest(ValidationError(Vector(parseError.toString)))
                  .pure[F]
              case Right(listOfJson) =>
                val bulkCreation = processBulkCreation(listOfJson)
                bulkCreation.map(bc => BulkCreationYamlResponse.Ok(bc))
    }
    res
  end bulkCreationYaml
end OntologyManagerHandler
