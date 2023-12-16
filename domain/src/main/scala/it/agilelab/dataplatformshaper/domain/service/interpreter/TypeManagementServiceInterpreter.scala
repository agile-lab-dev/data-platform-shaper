package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.*
import cats.data.*
import cats.effect.*
import cats.syntax.*
import cats.syntax.all.*
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.*
import it.agilelab.dataplatformshaper.domain.model.NS.*
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.NonExistentInstanceTypeError
import it.agilelab.dataplatformshaper.domain.service.{
  ManagementServiceError,
  TraitManagementService,
  TypeManagementService
}
import org.datatools.bigdatatypes.basictypes.SqlType
import org.eclipse.rdf4j.model.util.Statements.statement
import org.eclipse.rdf4j.model.util.Values.{iri, literal, triple}
import org.eclipse.rdf4j.model.vocabulary.{OWL, RDF, RDFS}
import org.eclipse.rdf4j.model.{IRI, Literal, Statement}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID
import scala.language.{implicitConversions, postfixOps}

class TypeManagementServiceInterpreter[F[_]: Sync](
    traitManagementService: TraitManagementService[F]
)(using cache: Ref[F, Map[String, EntityType]])
    extends TypeManagementService[F]:

  val repository: KnowledgeGraph[F] = traitManagementService.repository

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

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.defaultArgs"
    )
  )
  private def stringToDataType(
      stringType: String,
      stringMode: String,
      records: Option[List[(String, SqlType)]] = None
  ): SqlType =
    stringType match
      case "StringAttributeType" =>
        StringType(modeStringToMode(stringMode))
      case "IntAttributeType" =>
        IntType(modeStringToMode(stringMode))
      case "DateAttributeType" =>
        DateType(modeStringToMode(stringMode))
      case "TimestampAttributeType" =>
        TimestampDataType(modeStringToMode(stringMode))
      case "DoubleAttributeType" =>
        DoubleType(modeStringToMode(stringMode))
      case "FloatAttributeType" =>
        FloatType(modeStringToMode(stringMode))
      case "LongAttributeType" =>
        LongType(modeStringToMode(stringMode))
      case "BooleanAttributeType" =>
        BooleanType(modeStringToMode(stringMode))
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
      dataType: SqlType,
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
      case DateType(mode) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L2),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L2),
          statement(
            triple(childEntity, RDFS.RANGE, NS.DATEATTRIBUTETYPE),
            L2
          ),
          modeToStatement(childEntity, mode)
        )
      case TimestampDataType(mode) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L2),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L2),
          statement(
            triple(childEntity, RDFS.RANGE, NS.TIMESTAMPATTRIBUTETYPE),
            L2
          ),
          modeToStatement(childEntity, mode)
        )
      case DoubleType(mode) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L2),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L2),
          statement(
            triple(childEntity, RDFS.RANGE, NS.DOUBLEATTRIBUTETYPE),
            L2
          ),
          modeToStatement(childEntity, mode)
        )
      case FloatType(mode) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L2),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L2),
          statement(
            triple(childEntity, RDFS.RANGE, NS.FLOATATTRIBUTETYPE),
            L2
          ),
          modeToStatement(childEntity, mode)
        )
      case LongType(mode) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L2),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L2),
          statement(triple(childEntity, RDFS.RANGE, NS.LONGATTRIBUTETYPE), L2),
          modeToStatement(childEntity, mode)
        )
      case BooleanType(mode) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L2),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L2),
          statement(
            triple(childEntity, RDFS.RANGE, NS.BOOLEANATTRIBUTETYPE),
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

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.asInstanceOf"
    )
  )
  private def fetchStatementsForType(typeName: String): F[List[Statement]] = {
    val query: String =
      s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |SELECT ?s ?p ?o WHERE {
         | BIND(iri("${ns.getName}$typeName") as ?s)
         |  ?s ?p ?o .
         |}
         |""".stripMargin

    val statements: F[List[Statement]] =
      repository
        .evaluateQuery(query)
        .flatMap(ibs =>
          Traverse[List].sequence(
            ibs
              .map(bs => {
                val sb = bs.getBinding("s")
                val pb = bs.getBinding("p")
                val ob = bs.getBinding("o")
                val s = iri(sb.getValue.stringValue)
                val p = iri(pb.getValue.stringValue)
                if (ob.getValue.isLiteral)
                  summon[Applicative[F]].pure(
                    List(
                      statement(
                        triple(s, p, ob.getValue.asInstanceOf[Literal]),
                        L2
                      )
                    )
                  )
                else
                  summon[Applicative[F]].pure(
                    List(
                      statement(
                        triple(s, p, iri(ob.getValue.stringValue)),
                        L2
                      )
                    )
                  )
              })
              .toList
          )
        )
        .map(_.flatten)

    statements
  }

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
  ): F[List[(String, SqlType)]] =
    val query = queryForType(structIri)
    repository
      .evaluateQuery(query)
      .map(ibs =>
        val records: Iterator[F[(String, SqlType)]] = ibs.map(bs =>
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
  ): F[Either[ManagementServiceError, Set[String]]] =
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
            case iri: IRI => iri.getLocalName
        ).toSet
      )
      .map(Right[ManagementServiceError, Set[String]])
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

  private def createOrDelete(
      entityType: EntityType,
      isCreation: Boolean
  ): F[Either[ManagementServiceError, Unit]] =

    val instanceType = iri(ns, entityType.name)
    print(isCreation)
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
        s"About to ${if (isCreation) "create" else "delete"} an instance type with name ${entityType.name}"
      )
      exist <- EitherT(exist(entityType.name))
      _ <- traceT(
        s"Checking the existence, does ${entityType.name} already exist? $exist"
      )
      traitExistence <- EitherT(traitManagementService.exist(entityType.traits))
      _ <- {
        val nonExistentTraits =
          traitExistence.filter(!_._2).map(_._1).toList
        if (nonExistentTraits.isEmpty)
          EitherT.rightT[F, ManagementServiceError](())
        else
          EitherT.leftT[F, Unit](
            ManagementServiceError.NonExistentTraitError(nonExistentTraits.head)
          )
      }
      stmts <- EitherT(statementsForInheritance)
      _ <- EitherT {
        if !exist then
          if isCreation then
            summon[Functor[F]].map(
              repository.removeAndInsertStatements(
                stmts ++ traitsStatements ++ typeInstanceStatements ++ attributeStatements
              )
            )(Right[ManagementServiceError, Unit])
          else
            summon[Functor[F]].map(
              repository.removeAndInsertStatements(
                List.empty[Statement],
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
      _ <- traceT(s"Instance type ${if (isCreation) "created" else "deleted"}")
    } yield ()).value
  end createOrDelete

  override def create(
      entityType: EntityType,
      inheritsFrom: String
  ): F[Either[ManagementServiceError, Unit]] =
    given TypeManagementService[F] = this
    (for {
      enrichedEntityType <- EitherT(entityType.inheritsFrom(inheritsFrom))
      _ <- EitherT(createOrDelete(enrichedEntityType, true))
    } yield ()).value
  end create

  override def create(
      entityType: EntityType
  ): F[Either[ManagementServiceError, Unit]] =
    createOrDelete(entityType, true)
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

  private def hasInstances(
      instanceTypeName: String
  ): F[Either[ManagementServiceError, Boolean]] = {
    val instanceCheckQuery =
      s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |SELECT (COUNT(?instance) as ?instanceCount) WHERE {
         |   ?instance ns:isClassifiedBy ?type .
         |   ?type ns:typeName "$instanceTypeName"^^xsd:string .
         |}
         |""".stripMargin

    val instanceCheckResult = repository.evaluateQuery(instanceCheckQuery)
    summon[Functor[F]].map(instanceCheckResult) { resultSet =>
      val resultList = resultSet.toList
      Right(
        resultList.nonEmpty && resultList.head
          .getValue("instanceCount")
          .stringValue()
          .toInt > 0
      )
    }
  }

  override def delete(
      instanceTypeName: String
  ): F[Either[ManagementServiceError, Unit]] = {
    val existenceCheck = exist(instanceTypeName)
    summon[Monad[F]].flatMap(existenceCheck) {
      case Right(true) =>
        val instanceCheck = hasInstances(instanceTypeName)
        summon[Monad[F]].flatMap(instanceCheck) {
          case Right(true) =>
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Unit](
                ManagementServiceError.TypeHasInstancesError(
                  instanceTypeName
                )
              )
            )

          case Right(false) =>
            val statementsToRemoveF = fetchStatementsForType(instanceTypeName)
            summon[Monad[F]].flatMap(statementsToRemoveF) {
              statementsToRemove =>
                val res = repository.removeAndInsertStatements(
                  List.empty,
                  statementsToRemove
                )
                summon[Functor[F]].map(res)(_ =>
                  Right[ManagementServiceError, Unit](())
                )
            }

          case Left(error) =>
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Unit](error)
            )
        }

      case Right(false) =>
        summon[Applicative[F]].pure(
          Left[ManagementServiceError, Unit](
            NonExistentInstanceTypeError(instanceTypeName)
          )
        )

      case Left(error) =>
        summon[Applicative[F]].pure(Left[ManagementServiceError, Unit](error))
    }
  }

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
         |   ?a ns:typeName "$instanceTypeName"^^<http://www.w3.org/2001/XMLSchema#string> .
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
