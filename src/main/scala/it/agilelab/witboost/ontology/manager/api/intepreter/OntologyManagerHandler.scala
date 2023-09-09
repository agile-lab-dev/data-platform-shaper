package it.agilelab.witboost.ontology.manager.api.intepreter

import cats.*
import cats.data.*
import cats.effect.*
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging
import it.agilelab.witboost.ontology.manager.Resource.*
import it.agilelab.witboost.ontology.manager.definitions.{ValidationError, EntityType as IEntityType}
import it.agilelab.witboost.ontology.manager.domain.model.l0.EntityType
import it.agilelab.witboost.ontology.manager.domain.model.l1.{SpecificTrait, given}
import it.agilelab.witboost.ontology.manager.domain.model.schema.*
import it.agilelab.witboost.ontology.manager.domain.service.interpreter.{InstanceManagementServiceInterpreter, TypeManagementServiceInterpreter}
import it.agilelab.witboost.ontology.manager.{Handler, Resource}

import scala.util.Try

class OntologyManagerHandler[F[_]: Async](
    tms: TypeManagementServiceInterpreter[F],
    ims: InstanceManagementServiceInterpreter[F]
) extends Handler[F]
    with StrictLogging:
  override def create(
      respond: Resource.CreateResponse.type
  )(body: IEntityType): F[CreateResponse] =
    val schema = body.schema

    val traits =
      summon[Applicative[F]].pure(Try(
        body
          .traits
          .fold(Set.empty[SpecificTrait])(x => x.map(str => str: SpecificTrait).toSet)
      )
        .toEither
        .leftMap(
          t =>
            s"Trait ${t.getMessage} is not a Trait"
        ))
    val res = for {
      ts <- EitherT(traits)
      res <- EitherT(
        tms
          .create(
            EntityType(
              body.name,
              ts,
              StructType(List("name" -> StringType())): Schema,
              None
            )
          ).map(_.leftMap(_.getMessage))
      )
    } yield res

    res.value.map {
      case Left(error)  => respond.BadRequest(ValidationError(Vector(error)))
      case Right(_) => respond.Ok("OK")
    }.onError(t =>
      summon[Applicative[F]].pure(logger.error(s"Error: ${t.getMessage}")
      )
    )
  end create
end OntologyManagerHandler
