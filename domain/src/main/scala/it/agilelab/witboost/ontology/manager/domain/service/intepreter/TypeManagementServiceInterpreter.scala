package it.agilelab.witboost.ontology.manager.domain.service.intepreter

import cats.*
import cats.data.*
import cats.effect.*
import cats.syntax.*
import cats.syntax.all.*
import it.agilelab.witboost.ontology.manager.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.witboost.ontology.manager.domain.model.NS
import it.agilelab.witboost.ontology.manager.domain.model.NS.*
import it.agilelab.witboost.ontology.manager.domain.model.l0.EntityType
import it.agilelab.witboost.ontology.manager.domain.service.{
  ManagementServiceError,
  TypeManagementService
}
import it.agilelab.witboost.ontology.manager.domain.common.EitherTLogging.traceT
import it.agilelab.witboost.ontology.manager.domain.model.*
import it.agilelab.witboost.ontology.manager.domain.model.l0.*
import it.agilelab.witboost.ontology.manager.domain.model.l1.{*, given}
import it.agilelab.witboost.ontology.manager.domain.model.schema.*
import it.agilelab.witboost.ontology.manager.domain.model.schema.Mode.*
import org.eclipse.rdf4j.model.impl.DynamicModelFactory
import org.eclipse.rdf4j.model.util.Statements.statement
import org.eclipse.rdf4j.model.util.Values.{iri, literal, triple}
import org.eclipse.rdf4j.model.vocabulary.{OWL, RDF, RDFS}
import org.eclipse.rdf4j.model.{IRI, Statement}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID
import scala.language.{implicitConversions, postfixOps}

class TypeManagementServiceInterpreter[F[_]: Sync](
    val repository: KnowledgeGraph[F]
)(using cache: Ref[F, Map[String, EntityType]])
    extends TypeManagementService[F]:

  extension (entityType: EntityType)
    def inheritsFrom(fatherName: String)(using
        tms: TypeManagementService[F]
    ): F[Either[ManagementServiceError, EntityType]] =
      entityType.father.fold(
        tms
          .read(fatherName)
          .map(
            _.map(fatherEntityType =>
              entityType.copy(father = Some(fatherEntityType))
            )
          )
      )(_ =>
        summon[Applicative[F]].pure(
          Right[ManagementServiceError, EntityType](entityType)
        )
      )
    end inheritsFrom
  end extension

  given logger: Logger[F] = Slf4jLogger.getLogger[F]

  private def modeToStatement(entity: IRI, mode: Mode): Statement =
    mode match
      case Nullable =>
        statement(triple(entity, NS.MODE, NS.NULLABLE), L2)
      case Repeated =>
        statement(triple(entity, NS.MODE, NS.REPEATED), L2)
      case Required =>
        statement(triple(entity, NS.MODE, NS.REQUIRED), L2)
    end match
  end modeToStatement

  private def stringToDataType(
      stringType: String,
      stringMode: String,
      records: Option[List[(String, DataType)]] = None
  ): DataType =
    stringType match
      case "StringAttributeType" =>
        StringType(modeStringToMode(stringMode))
      case "IntAttributeType" =>
        IntType(modeStringToMode(stringMode))
      case _ =>
        StructType(records.getOrElse(List.empty), modeStringToMode(stringMode))
  end stringToDataType

  private def modeStringToMode(modeString: String): Mode =
    modeString match
      case "Required" => Required
      case "Repeated" => Repeated
      case "Nullable" => Nullable
    end match
  end modeStringToMode

  private def isStructType(fieldType: String): Boolean =
    fieldType match
      case "StringAttributeType" | "IntAttributeType" => false
      case _                                          => true
  end isStructType
  
  private def emitStatements(
      fatherEntity: IRI,
      dataType: DataType,
      childEntity: IRI,
      currentPath: String
  ): List[Statement] =
    dataType match
      case StructType(records, mode) =>
        val structTypeInstance = iri(ns, UUID.randomUUID().toString)
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L2),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L2),
          statement(triple(childEntity, RDFS.RANGE, structTypeInstance), L2),
          statement(triple(structTypeInstance, RDF.TYPE, NS.STRUCTTYPE), L2),
          modeToStatement(childEntity, mode)
        ) ++ records.flatMap(record =>
          emitStatements(
            structTypeInstance,
            record(1),
            iri(ns, s"$currentPath/${record(0)}"),
            s"$currentPath/${record(0)}"
          )
        )
      case StringType(mode) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L2),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L2),
          statement(
            triple(childEntity, RDFS.RANGE, NS.STRINGATTRIBUTETYPE),
            L2
          ),
          modeToStatement(childEntity, mode)
        )
      case IntType(mode) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L2),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L2),
          statement(triple(childEntity, RDFS.RANGE, NS.INTATTRIBUTETYPE), L2),
          modeToStatement(childEntity, mode)
        )
      case _ =>
        List.empty[Statement]
    end match
  end emitStatements

  private def emitStatementsFromSchema(
      instanceType: IRI,
      schema: Schema
  ): List[Statement] =
    schema.records.flatMap(record =>
      emitStatements(
        instanceType,
        record(1),
        iri(ns, s"${instanceType.getLocalName}/${record(0)}"),
        s"${instanceType.getLocalName}/${record(0)}"
      )
    )
  end emitStatementsFromSchema

  private def queryForType(entityType: IRI): String =
    s"""
       |PREFIX ns:  <${ns.getName}>
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |SELECT ?field ?type ?mode WHERE {
       |    BIND(iri("${entityType.stringValue()}") as ?entityType)
       |    ?field rdfs:domain ?entityType .
       |    ?field rdfs:range  ?type .
       |    ?field ns:mode ?mode .
       |  }
       |""".stripMargin
  end queryForType

  private def getStructTypeRecords(
      structIri: IRI
  ): F[List[(String, DataType)]] =
    val query = queryForType(structIri)
    repository
      .evaluateQuery(query)
      .map(ibs =>
        val records: Iterator[F[(String, DataType)]] = ibs.map(bs =>
          val fieldName =
            iri(bs.getBinding("field").getValue.stringValue()).getLocalName
          val fieldType =
            iri(bs.getBinding("type").getValue.stringValue()).getLocalName
          val fieldMode =
            iri(bs.getBinding("mode").getValue.stringValue()).getLocalName
          if !isStructType(fieldType) then
            summon[Applicative[F]]
              .pure(fieldName -> stringToDataType(fieldType, fieldMode, None))
          else
            getStructTypeRecords(
              iri(bs.getBinding("type").getValue.stringValue())
            )
              .map(records =>
                fieldName -> stringToDataType(
                  fieldType,
                  fieldMode,
                  Some(records)
                )
              )
          end if
        )
        Traverse[List].sequence(records.toList)
      )
      .flatten
  end getStructTypeRecords

  private def getFatherFromEntityType(
      entityTypeName: String
  ): F[Either[ManagementServiceError, Option[EntityType]]] =
    val entityTypeIri = iri(ns, entityTypeName)
    val query: String =
      s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |SELECT ?father WHERE {
         | BIND(iri("${entityTypeIri.stringValue()}") as ?entityType)
         |    ?entityType ns:inheritsFrom ?father .
         |  }
         |""".stripMargin
    (for {
      fatherName <- repository
        .evaluateQuery(query)
        .map(
          _.map(bs =>
            bs.getBinding("father").getValue match
              case iri: IRI => iri.getLocalName
          ).toList.headOption
        )
    } yield fatherName match
      case None =>
        summon[Applicative[F]].pure(
          Right[ManagementServiceError, Option[EntityType]](
            None: Option[EntityType]
          )
        ): F[Either[ManagementServiceError, Option[EntityType]]]
      case Some(name) =>
        read(name).map(_.map(et => Some(et)))
    ).flatten
  end getFatherFromEntityType

  private def getTraitsFromEntityType(
      entityTypeName: String
  ): F[Either[ManagementServiceError, Set[SpecificTrait]]] =
    val entityTypeIri = iri(ns, entityTypeName)
    val query: String =
      s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |SELECT ?trait WHERE {
         | BIND(iri("${entityTypeIri.stringValue()}") as ?entityType)
         |    ?entityType ns:hasTrait ?trait .
         |  }
         |""".stripMargin
    repository
      .evaluateQuery(query)
      .map(
        _.map(bs =>
          bs.getBinding("trait").getValue match
            case iri: IRI => iri.getLocalName: SpecificTrait
        ).toSet
      )
      .map(Right[ManagementServiceError, Set[SpecificTrait]])
  end getTraitsFromEntityType

  private def getSchemaFromEntityType(entityTypeName: String): F[Schema] =
    val entityTypeIri = iri(ns, entityTypeName)
    val query = queryForType(entityTypeIri)
    repository
      .evaluateQuery(query)
      .map(ibs =>
        Traverse[List].sequence(
          ibs
            .map(bs =>
              val fieldName =
                iri(bs.getBinding("field").getValue.stringValue()).getLocalName
              val fieldType =
                iri(bs.getBinding("type").getValue.stringValue()).getLocalName
              val fieldMode =
                iri(bs.getBinding("mode").getValue.stringValue()).getLocalName
              if !isStructType(fieldType) then
                summon[Applicative[F]].pure(
                  fieldName -> stringToDataType(fieldType, fieldMode, None)
                )
              else
                getStructTypeRecords(
                  iri(bs.getBinding("type").getValue.stringValue())
                )
                  .map(records =>
                    fieldName -> stringToDataType(
                      fieldType,
                      fieldMode,
                      Some(records)
                    )
                  )
              end if
            )
            .toList
        )
      )
      .flatMap(_.map(StructType(_, Required)))
  end getSchemaFromEntityType

  override def create(
      entityType: EntityType
  ): F[Either[ManagementServiceError, Unit]] =

    val model = (new DynamicModelFactory).createEmptyModel()

    model.setNamespace(ns)
    model.setNamespace(OWL.NS)
    model.setNamespace(RDF.NS)
    model.setNamespace(RDFS.NS)

    val instanceType = iri(ns, entityType.name)

    val statementsForInheritance
        : F[Either[ManagementServiceError, List[Statement]]] =
      entityType.father.fold(
        summon[Applicative[F]].pure(
          Right[ManagementServiceError, List[Statement]](List.empty[Statement])
        )
      )(entityType =>
        exist(entityType.name).map(
          _.map(exist =>
            if exist then
              List(
                statement(
                  triple(instanceType, INHERITSFROM, iri(ns, entityType.name)),
                  L2
                )
              )
            else List.empty[Statement]
          )
        )
      )

    val traitsStatements = entityType.traits
      .map(`trait` =>
        statement(
          triple(instanceType, NS.HASTRAIT, iri(ns, `trait`: String)),
          L2
        )
      )
      .toList

    val typeInstanceStatements = List(
      statement(triple(instanceType, RDF.TYPE, OWL.NAMEDINDIVIDUAL), L2),
      statement(triple(instanceType, RDF.TYPE, ENTITYTYPE), L2),
      statement(triple(instanceType, TYPENAME, literal(entityType.name)), L2)
    )

    val attributeStatements =
      emitStatementsFromSchema(instanceType, entityType.baseSchema)
    (for {
      _ <- traceT(
        s"About to create an instance type with name ${entityType.name}"
      )
      exist <- EitherT(exist(entityType.name))
      _ <- traceT(
        s"Checking the existence, does ${entityType.name} already exist? $exist"
      )
      stmts <- EitherT(statementsForInheritance)
      _ <- EitherT {
        if !exist
        then
          summon[Functor[F]].map(
            repository.removeAndInsertStatements(
              stmts ++ traitsStatements ++ typeInstanceStatements ++ attributeStatements
            )
          )(Right[ManagementServiceError, Unit])
        else
          summon[Applicative[F]]
            .pure(
              Left[ManagementServiceError, Unit](
                ManagementServiceError.TypeAlreadyDefinedError(entityType.name)
              )
            )
      }
      _ <- traceT(s"Instance type created")
    } yield ()).value
  end create

  override def create(
      entityType: EntityType,
      inheritsFrom: String
  ): F[Either[ManagementServiceError, Unit]] =
    given TypeManagementService[F] = this
    (for {
      enrichedEntityType <- EitherT(entityType.inheritsFrom(inheritsFrom))
      _ <- EitherT(create(enrichedEntityType))
    } yield ()).value
  end create

  override def read(
      instanceTypeName: String
  ): F[Either[ManagementServiceError, EntityType]] =
    (for {
      _ <- traceT(s"read $instanceTypeName")
      exist <- EitherT(exist(instanceTypeName))
      _ <- traceT(s"read $instanceTypeName: existence check true/false: $exist")
      definitionWithExistCheck <- EitherT {
        if exist
        then
          (for {
            retrieved <- EitherT(
              summon[Functor[F]].map(cache.get)(m =>
                Right[ManagementServiceError, Option[EntityType]](
                  m.get(instanceTypeName)
                )
              )
            )
            _ <- traceT(
              s"read $instanceTypeName: check in the cache $retrieved"
            )
            definitionWithCacheLookup <- EitherT(
              retrieved
                .fold(
                  (for {
                    _ <- traceT(
                      s"read $instanceTypeName: not found in the cache"
                    )
                    traits <- EitherT(getTraitsFromEntityType(instanceTypeName))
                    _ <- traceT(
                      s"read $instanceTypeName: retrieved traits $traits"
                    )
                    father <- EitherT(getFatherFromEntityType(instanceTypeName))
                    _ <- traceT(
                      s"read $instanceTypeName: retrieved father $father if any"
                    )
                    entityType <- EitherT(
                      getSchemaFromEntityType(instanceTypeName).map(schema =>
                        Right[ManagementServiceError, EntityType](
                          EntityType(
                            instanceTypeName,
                            traits,
                            schema,
                            father
                          )
                        )
                      )
                    )
                    _ <- traceT(
                      s"read $instanceTypeName: candidate $entityType"
                    )
                    et <- EitherT(
                      cache
                        .modify(map =>
                          (map + (instanceTypeName -> entityType), entityType)
                        )
                        .map(Right[ManagementServiceError, EntityType])
                    )
                  } yield et).value
                )(et =>
                  logger.trace(s"found in the cache") *> summon[Applicative[F]]
                    .pure(Right[ManagementServiceError, EntityType](et))
                )
            )
          } yield definitionWithCacheLookup).value
        else
          summon[Applicative[F]].pure(
            Left[ManagementServiceError, EntityType](
              ManagementServiceError.NonExistentInstanceTypeError(
                instanceTypeName
              )
            )
          )
        end if
      }
    } yield definitionWithExistCheck).value
  end read

  override def exist(
      instanceTypeName: String
  ): F[Either[ManagementServiceError, Boolean]] =
    val res = repository.evaluateQuery(s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |SELECT ?a WHERE {
         |   ?a rdf:type owl:NamedIndividual .
         |   ?a rdf:type ns:EntityType .
         |   ?a ns:typeName "$instanceTypeName" .
         |}
         |""".stripMargin)
    summon[Functor[F]].map(res)(res => {
      val count = res.toList.length
      if count > 0
      then Right[ManagementServiceError, Boolean](true)
      else Right[ManagementServiceError, Boolean](false)
    })
  end exist
end TypeManagementServiceInterpreter
