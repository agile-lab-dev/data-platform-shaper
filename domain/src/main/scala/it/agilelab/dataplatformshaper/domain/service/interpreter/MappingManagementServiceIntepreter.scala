package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.*
import cats.data.*
import cats.effect.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.NS
import it.agilelab.dataplatformshaper.domain.model.NS.{L2, ns}
import it.agilelab.dataplatformshaper.domain.model.l1.{*, given}
import it.agilelab.dataplatformshaper.domain.model.schema.{
  schemaToMapperSchema,
  validateMappingTuple
}
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.*
import it.agilelab.dataplatformshaper.domain.service.{
  ManagementServiceError,
  MappingManagementService,
  TypeManagementService
}
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.model.util.Statements.statement
import org.eclipse.rdf4j.model.util.Values.{iri, literal, triple}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID

class MappingManagementServiceIntepreter[F[_]: Sync](
    typeManagementService: TypeManagementService[F]
) extends MappingManagementService[F]
    with InstanceManagementServiceInterpreterCommonFunctions[F]:

  given logger: Logger[F] = Slf4jLogger.getLogger[F]

  val repository: KnowledgeGraph[F] = typeManagementService.repository

  override def create(
      mappingName: String,
      sourceEntityTypeName: String,
      targetEntityTypeName: String,
      mapper: Tuple
  ): F[Either[ManagementServiceError, Unit]] =

    val sourceEntityTypeIri = iri(ns, sourceEntityTypeName)
    val targetEntityTypeIri = iri(ns, targetEntityTypeName)
    val mapperId = UUID.randomUUID().toString
    val mapperIri = iri(ns, mapperId)

    val mappedToTriple = triple(
      sourceEntityTypeIri,
      iri(mappedTo.getNamespace, mappedTo),
      targetEntityTypeIri
    )

    val mappedFromTriple = triple(
      targetEntityTypeIri,
      iri(mappedFrom.getNamespace, mappedFrom),
      sourceEntityTypeIri
    )

    val statements = List(
      statement(
        mappedToTriple,
        NS.MAPPINGNAME,
        literal(mappingName),
        L2
      ),
      statement(
        mappedToTriple,
        NS.MAPPEDBY,
        mapperIri,
        L2
      ),
      statement(
        mappedFromTriple,
        NS.MAPPINGNAME,
        literal(mappingName),
        L2
      ),
      statement(
        mappedFromTriple,
        NS.MAPPEDBY,
        mapperIri,
        L2
      )
    )

    (for {
      _ <- EitherT(
        summon[Functor[F]]
          .map(exist(mappingName, sourceEntityTypeName, targetEntityTypeName))(
            _.flatMap(exist =>
              if exist then
                Left[ManagementServiceError, Unit](InvalidMappingError(""))
              else Right[ManagementServiceError, Unit](())
            )
          )
      )
      ttype <- EitherT(typeManagementService.read(targetEntityTypeName))
      _ <- EitherT(
        summon[Applicative[F]].pure(
          validateMappingTuple(mapper, ttype.schema).leftMap(error =>
            MapperInstanceValidationError(error)
          )
        )
      )
      stmts <- EitherT(
        summon[Applicative[F]].pure(
          emitStatementsForEntity(
            mapperId,
            statements,
            mapper,
            schemaToMapperSchema(ttype.schema),
            L2
          )
        )
      )
      res <- EitherT(
        summon[Functor[F]].map(
          repository.removeAndInsertStatements(stmts, List.empty[Statement])
        )(Right[ManagementServiceError, Unit])
      )
    } yield res).value
  end create

  override def exist(
      mappingName: String,
      sourceEntityTypeName: String,
      targetEntityTypeName: String
  ): F[Either[ManagementServiceError, Boolean]] =
    val query = s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |select ?s where {
         |  <<ns:$sourceEntityTypeName ns:mappedTo ns:$targetEntityTypeName>> ns:mappingName "$mappingName"^^xsd:string .
         |  }
         |""".stripMargin
    val res = repository.evaluateQuery(query)
    summon[Functor[F]].map(res)(res =>
      val count = res.toList.length
      if count > 0 then Right[ManagementServiceError, Boolean](true)
      else Right[ManagementServiceError, Boolean](false)
      end if
    )
  end exist

end MappingManagementServiceIntepreter
