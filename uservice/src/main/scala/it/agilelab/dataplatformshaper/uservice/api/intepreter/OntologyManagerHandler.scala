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
import it.agilelab.dataplatformshaper.domain.model.l0.EntityType
import it.agilelab.dataplatformshaper.domain.model.l1.{
  SpecificTrait,
  given
}
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import it.agilelab.dataplatformshaper.uservice.Resource.CreateTypeResponse
import it.agilelab.dataplatformshaper.uservice.definitions.{
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
    ims: InstanceManagementServiceInterpreter[F]
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
            .fold(Set.empty[SpecificTrait])(x =>
              x.map(str => str: SpecificTrait).toSet
            )
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
      ts = entityType.traits.fold(Set.empty[SpecificTrait])(x =>
        x.map(str => str: SpecificTrait).toSet
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

end OntologyManagerHandler
