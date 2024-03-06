package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.*
import cats.data.*
import cats.effect.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.NS
import it.agilelab.dataplatformshaper.domain.model.NS.{L2, L3, ns}
import it.agilelab.dataplatformshaper.domain.model.l1.{*, given}
import it.agilelab.dataplatformshaper.domain.model.mapping.{
  MappingDefinition,
  MappingKey
}
import it.agilelab.dataplatformshaper.domain.model.schema.{
  schemaToMapperSchema,
  tupleToMappedTuple,
  validateMappingTuple
}
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.*
import it.agilelab.dataplatformshaper.domain.service.{
  InstanceManagementService,
  ManagementServiceError,
  MappingManagementService,
  TypeManagementService
}
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.model.util.Statements.statement
import org.eclipse.rdf4j.model.util.Values.{iri, triple}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID

class MappingManagementServiceInterpreter[F[_]: Sync](
    typeManagementService: TypeManagementService[F],
    instanceManagementService: InstanceManagementService[F]
) extends MappingManagementService[F]
    with InstanceManagementServiceInterpreterCommonFunctions[F]:

  given logger: Logger[F] = Slf4jLogger.getLogger[F]

  val repository: KnowledgeGraph[F] = typeManagementService.repository

  override def create(
      mappingDefinition: MappingDefinition
  ): F[Either[ManagementServiceError, Unit]] =
    val key = mappingDefinition.mappingKey
    val mapper = mappingDefinition.mapper
    val sourceEntityTypeIri = iri(ns, key.sourceEntityTypeName)
    val targetEntityTypeIri = iri(ns, key.targetEntityTypeName)
    val mapperId = UUID.randomUUID().toString
    val mapperIri = iri(ns, mapperId)

    val mappedToTriple1 = triple(
      sourceEntityTypeIri,
      iri(mappedTo.getNamespace, s"${mappedTo: String}#${key.mappingName}"),
      targetEntityTypeIri
    )

    val mappedToTriple2 = triple(
      iri(mappedTo.getNamespace, s"${mappedTo: String}#${key.mappingName}"),
      iri(ns, "singletonPropertyOf"),
      iri(mappedTo.getNamespace, mappedTo)
    )

    val mappedToTriple3 = triple(
      iri(mappedTo.getNamespace, s"${mappedTo: String}#${key.mappingName}"),
      NS.MAPPEDBY,
      mapperIri
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
      )
    )

    (for {
      _ <- traceT(
        s"About to create a mapping named ${mappingDefinition.mappingKey.mappingName} with source ${key.sourceEntityTypeName}, target ${key.targetEntityTypeName} with the mapper tuple:\n ${mappingDefinition.mapper} "
      )
      _ <- EitherT(
        summon[Functor[F]]
          .map(exist(key))(
            _.flatMap(exist =>
              if exist then
                Left[ManagementServiceError, Unit](
                  InvalidMappingError(
                    s"the mapping with name ${key.mappingName}, source type ${key.sourceEntityTypeName} and target type ${key.targetEntityTypeName} already exists"
                  )
                )
              else Right[ManagementServiceError, Unit](())
            )
          )
      )
      _ <- EitherT(
        summon[Functor[F]].map(
          typeManagementService.read(key.sourceEntityTypeName)
        )(
          _.flatMap(t =>
            if t.traits("MappingSource") then
              Right[ManagementServiceError, Unit](())
            else
              Left[ManagementServiceError, Unit](
                InvalidMappingError(
                  s"in the mapping with name ${key.mappingName}, the source type ${t.name} doesn't contain the trait MappingSource"
                )
              )
          )
        )
      )
      _ <- EitherT(
        summon[Functor[F]].map(
          typeManagementService.read(key.targetEntityTypeName)
        )(
          _.flatMap(t =>
            if t.traits("MappingTarget") then
              Right[ManagementServiceError, Unit](())
            else
              Left[ManagementServiceError, Unit](
                InvalidMappingError(
                  s"in the mapping with name ${key.mappingName}, the target type ${t.name} doesn't contain the trait MappingTarget"
                )
              )
          )
        )
      )
      ttype <- EitherT(typeManagementService.read(key.targetEntityTypeName))
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

  override def createMappedInstances(
      sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]] =
    (for {
      sourceInstance <- EitherT(
        instanceManagementService.read(sourceInstanceId)
      )
      mappings <- EitherT(
        getMappingsForEntityType(
          logger,
          typeManagementService,
          sourceInstance.entityTypeName
        )
      )
      ids <- EitherT(
        summon[Functor[F]].map(Traverse[List].sequence(mappings.map(mapping =>
          val tuple: Either[ManagementServiceError, Tuple] = tupleToMappedTuple(
            sourceInstance.values,
            mapping(1).schema,
            mapping(3),
            mapping(2).schema
          ).leftMap(e => MapperInstanceValidationError(e))
          val targetInstanceId = UUID.randomUUID().toString
          val sourceEntityIri = iri(ns, sourceInstanceId)
          val targetEntityIri = iri(ns, targetInstanceId)
          val mapperIri = iri(ns, mapping(4))
          val mappedToTriple1 = triple(
            sourceEntityIri,
            iri(mappedTo.getNamespace, s"${mappedTo: String}#${mapping(0)}"),
            targetEntityIri
          )
          val mappedToTriple2 = triple(
            iri(mappedTo.getNamespace, s"${mappedTo: String}#${mapping(0)}"),
            iri(ns, "singletonPropertyOf"),
            iri(mappedTo.getNamespace, mappedTo)
          )
          val mappedToTriple3 = triple(
            iri(mappedTo.getNamespace, s"${mappedTo: String}#${mapping(0)}"),
            NS.MAPPEDBY,
            mapperIri
          )
          val initialStatements = List(
            statement(mappedToTriple1, L3),
            statement(mappedToTriple2, L3),
            statement(mappedToTriple3, L3)
          )
          summon[Functor[F]].map(
            tuple
              .map(
                createInstanceNoCheck(
                  logger,
                  typeManagementService,
                  targetInstanceId,
                  mapping(2).name,
                  _,
                  initialStatements,
                  List.empty
                )
              )
              .sequence
          )(_.flatten)
        )))(_.sequence)
      )
      _ <- EitherT(
        summon[Functor[F]]
          .map(ids.map(id => createMappedInstances(id)).sequence)(_.sequence)
      )
    } yield ()).value
  end createMappedInstances

  override def updateMappedInstances(
      sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]] =
    (for {
      mappings <- EitherT(
        getMappingsForEntity(
          logger,
          typeManagementService,
          instanceManagementService,
          sourceInstanceId
        )
      )
      ids <- EitherT(
        summon[Functor[F]].map(Traverse[List].sequence(mappings.map(mapping =>
          val tuple: Either[ManagementServiceError, Tuple] = tupleToMappedTuple(
            mapping(1).values,
            mapping(0).schema,
            mapping(4),
            mapping(2).schema
          ).leftMap(e => MapperInstanceValidationError(e))
          summon[Functor[F]].map(
            tuple
              .map(
                instanceManagementService.update(
                  mapping(3).entityId,
                  _
                )
              )
              .sequence
          )(_.flatten)
        )))(_.sequence)
      )
      _ <- EitherT(
        summon[Functor[F]]
          .map(ids.map(id => updateMappedInstances(id)).sequence)(_.sequence)
      )
    } yield ()).value
  end updateMappedInstances

  override def exist(
      mapperKey: MappingKey
  ): F[Either[ManagementServiceError, Boolean]] =
    val query = s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |SELECT ?mr WHERE {
         |   ?mr ns:singletonPropertyOf ns:mappedTo .
         |   ns:${mapperKey.sourceEntityTypeName} ?mr ns:${mapperKey.targetEntityTypeName} .
         |   FILTER(STR(?mr) = "${ns.getName}mappedTo#${mapperKey.mappingName}")
         |}
         |""".stripMargin
    val res = logger.trace(
      s"Checking the mapping existence with name ${mapperKey.mappingName} with the query:\n$query"
    ) *> repository.evaluateQuery(query)
    summon[Functor[F]].map(res)(res =>
      val count = res.toList.length
      if count > 0 then Right[ManagementServiceError, Boolean](true)
      else Right[ManagementServiceError, Boolean](false)
      end if
    )
  end exist

end MappingManagementServiceInterpreter
