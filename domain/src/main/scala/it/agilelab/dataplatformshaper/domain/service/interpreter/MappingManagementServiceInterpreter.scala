package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.*
import cats.data.*
import cats.effect.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.NS
import it.agilelab.dataplatformshaper.domain.model.NS.{L2, ns}
import it.agilelab.dataplatformshaper.domain.model.l1.{*, given}
import it.agilelab.dataplatformshaper.domain.model.schema.{schemaToMapperSchema, validateMappingTuple}
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.*
import it.agilelab.dataplatformshaper.domain.service.{ManagementServiceError, MappingManagementService, TypeManagementService}
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.model.util.Statements.statement
import org.eclipse.rdf4j.model.util.Values.{iri, triple}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID

class MappingManagementServiceInterpreter[F[_]: Sync](
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

    val mappedToTriple1 = triple(
      sourceEntityTypeIri,
      iri(mappedTo.getNamespace, s"${mappedTo: String}#$mappingName"),
      targetEntityTypeIri
    )

    val mappedToTriple2 = triple(
      iri(mappedTo.getNamespace, s"${mappedTo: String}#$mappingName"),
      iri(ns, "singletonPropertyOf"),
      iri(mappedTo.getNamespace, mappedTo)
    )

    val mappedToTriple3 = triple(
      iri(mappedTo.getNamespace, s"${mappedTo: String}#$mappingName"),
      NS.MAPPEDBY,
      mapperIri
    )

    val mappedFromTriple1 = triple(
      iri(mappedTo.getNamespace, s"${mappedTo: String}#$mappingName"),
      iri(ns, "hasSource"),
      sourceEntityTypeIri
    )

    val initialStatements = List(
      statement(
        mappedToTriple1,
        L2
      ),
      statement(
        mappedToTriple2,
        L2
      ),
      statement(
        mappedToTriple3,
        L2
      ),
      statement(
        mappedFromTriple1,
        L2
      )
    )

    (for {
      _ <- traceT(s"About to create a mapping named $mappingName with source $sourceEntityTypeName, target $targetEntityTypeName with the mapper tuple:\n $mapper ")
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
            ttype.name,
            mapper,
            schemaToMapperSchema(ttype.schema),
            L2
          )
        )
      )
      res <- EitherT(
        summon[Functor[F]].map(
          repository.removeAndInsertStatements(
            initialStatements ::: stmts,
            List.empty[Statement]
          )
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
         |SELECT ?pi WHERE {
         |   ?pi ns:singletonPropertyOf ns:mappedTo .
         |   ?pi ns:hasSource ns:$sourceEntityTypeName .
         |   FILTER(STR(?pi) = "${ns.getName}mappedTo#$mappingName")
         |}
         |""".stripMargin
    val res = logger.trace(s"Checking the mapping existence with name $mappingName with the query:\n$query") *> repository.evaluateQuery(query)
    summon[Functor[F]].map(res)(res =>
      val count = res.toList.length
      if count > 0 then
        Right[ManagementServiceError, Boolean](true)
      else
        Right[ManagementServiceError, Boolean](false)
      end if
    )
  end exist

end MappingManagementServiceInterpreter
