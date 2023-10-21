package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.*
import cats.data.*
import cats.effect.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.*
import it.agilelab.dataplatformshaper.domain.model.NS.*
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.model.l1.{*, given}
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.model.schema.parsing.FoldingPhase
import it.agilelab.dataplatformshaper.domain.model.schema.parsing.FoldingPhase.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.*
import it.agilelab.dataplatformshaper.domain.service.{
  InstanceManagementService,
  ManagementServiceError,
  TypeManagementService
}
import java.time.{LocalDate, ZonedDateTime}
import org.eclipse.rdf4j.model.util.Statements.statement
import org.eclipse.rdf4j.model.util.Values.{iri, literal, triple}
import org.eclipse.rdf4j.model.vocabulary.RDF
import org.eclipse.rdf4j.model.{IRI, Literal, Statement}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID
import scala.language.{implicitConversions, postfixOps}

class InstanceManagementServiceInterpreter[F[_]: Sync](
    typeManagementService: TypeManagementService[F]
) extends InstanceManagementService[F]:

  given logger: Logger[F] = Slf4jLogger.getLogger[F]

  val repository: KnowledgeGraph[F] = typeManagementService.repository

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.var"
    )
  )
  private def createInstanceNoCheck(
      entityId: String,
      instanceTypeName: String,
      tuple: Tuple,
      statementsToRemove: List[Statement]
  ): F[Either[ManagementServiceError, String]] =

    val entity = iri(ns, entityId)

    var previousEntityIri = entity
    var currentEntityIri = entity
    var statements = List.empty[Statement]

    statements = statement(
      triple(entity, NS.ISCLASSIFIEDBY, iri(ns, instanceTypeName)),
      L3
    ) :: statements
    statements =
      statement(triple(entity, RDF.TYPE, NS.ENTITY), L3) :: statements

    @SuppressWarnings(
      Array(
        "scalafix:DisableSyntax.asInstanceOf"
      )
    )
    def emitStatement(
        currentPath: String,
        tpe: DataType,
        value: Any,
        foldingPhase: FoldingPhase
    ): Unit =
      tpe match
        case StringType(mode) =>
          val lit =
            mode match
              case Required | Repeated => literal(value.asInstanceOf[String])
              case Nullable =>
                value
                  .asInstanceOf[Option[String]]
                  .fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            L3
          ) :: statements
        case IntType(mode) =>
          val lit =
            mode match
              case Required | Repeated =>
                literal(value.asInstanceOf[Int])
              case Nullable =>
                value.asInstanceOf[Option[Int]].fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            L3
          ) :: statements
        case DateType(mode) =>
          val lit =
            mode match
              case Required | Repeated =>
                literal(value.asInstanceOf[LocalDate])
              case Nullable =>
                value
                  .asInstanceOf[Option[LocalDate]]
                  .fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            L3
          ) :: statements
        case TimestampDataType(mode) =>
          val lit =
            mode match
              case Required | Repeated =>
                literal(value.asInstanceOf[ZonedDateTime].toString)
              case Nullable =>
                value
                  .asInstanceOf[Option[ZonedDateTime]]
                  .map(_.toString)
                  .fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            L3
          ) :: statements
        case DoubleType(mode) =>
          val lit =
            mode match
              case Required | Repeated =>
                literal(value.asInstanceOf[Double].toString)
              case Nullable =>
                value
                  .asInstanceOf[Option[Double]]
                  .fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            L3
          ) :: statements
        case FloatType(mode) =>
          val lit =
            mode match
              case Required | Repeated =>
                literal(value.asInstanceOf[Float].toString)
              case Nullable =>
                value.asInstanceOf[Option[Float]].fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            L3
          ) :: statements
        case StructType(attributes, mode) =>
          foldingPhase match
            case BeginFoldingStruct =>
              val structIri = iri(ns, UUID.randomUUID.toString)
              statements = statement(
                triple(currentEntityIri, iri(ns, currentPath), structIri),
                L3
              ) :: statements
              previousEntityIri = currentEntityIri
              currentEntityIri = structIri
            case EndFoldingStruct =>
              currentEntityIri = previousEntityIri
            case _ =>
              ()
          end match
        case _ =>
          ()
      end match
    end emitStatement

    val getSchema: F[Either[ManagementServiceError, Schema]] =
      summon[Functor[F]].map(typeManagementService.read(instanceTypeName))(
        _.map(_.schema)
      )

    (for {
      schema <- EitherT[F, ManagementServiceError, Schema](getSchema)
      _ <- traceT(s"Retrieved schema $schema for type name $instanceTypeName")
      es <- EitherT[F, ManagementServiceError, String](
        summon[Applicative[F]].pure(
          unfoldTuple(
            tuple,
            schema,
            emitStatement
          ) match
            case Left(parsingError) =>
              Left[ManagementServiceError, String](
                TupleIsNotConformToSchema(parsingError)
              )
            case Right(_) =>
              Right[ManagementServiceError, String]("")
        )
      )
      _ <- traceT(s"Statements emitted $es")
      id <- EitherT[F, ManagementServiceError, String](
        summon[Functor[F]].map(
          repository.removeAndInsertStatements(
            statements,
            statementsToRemove.map(st =>
              (st.getSubject, st.getPredicate, st.getObject)
            )
          )
        )(_ => Right[ManagementServiceError, String](entityId))
      )
      _ <- traceT(
        s"Statements emitted creating the instance $id:\n${statements.mkString("\n")}\n"
      )
    } yield id).value
  end createInstanceNoCheck

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.asInstanceOf",
      "scalafix:DisableSyntax.=="
    )
  )
  private def fetchStatementsForInstance(
      instanceId: String
  ): F[List[Statement]] =
    val query: String =
      s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |SELECT ?s ?p ?o WHERE {
         | BIND(iri("${ns.getName}$instanceId") as ?s)
         |    ?s ?p ?o .
         |  }
         |""".stripMargin
    val statements: F[List[Statement]] =
      repository
        .evaluateQuery(query)
        .flatMap(ibs =>
          Traverse[List].sequence(
            ibs
              .map(bs =>
                val sb = bs.getBinding("s")
                val pb = bs.getBinding("p")
                val ob = bs.getBinding("o")
                val s = iri(sb.getValue.stringValue)
                val p = iri(pb.getValue.stringValue)
                if ob.getValue.isLiteral
                then
                  summon[Applicative[F]].pure(
                    List(
                      statement(
                        triple(s, p, ob.getValue.asInstanceOf[Literal]),
                        L3
                      )
                    )
                  )
                else
                  if p == RDF.TYPE || p == NS.ISCLASSIFIEDBY
                  then
                    summon[Applicative[F]].pure(
                      List(
                        statement(
                          triple(s, p, iri(ob.getValue.stringValue)),
                          L3
                        )
                      )
                    )
                  else
                    val stmt = statement(
                      triple(s, p, iri(ob.getValue.stringValue)),
                      L3
                    )
                    fetchStatementsForInstance(
                      iri(ob.getValue.stringValue).getLocalName
                    ).map(statements => stmt :: statements)
                  end if
                end if
              )
              .toList
          )
        )
        .map(_.flatten)
    statements
  end fetchStatementsForInstance

  override def create(
      instanceTypeName: String,
      values: Tuple
  ): F[Either[ManagementServiceError, String]] =
    val entityId = UUID.randomUUID().toString
    (for {
      exist <- EitherT(typeManagementService.exist(instanceTypeName))
      result <- EitherT {
        if exist
        then
          createInstanceNoCheck(entityId, instanceTypeName, values, List.empty)
        else
          summon[Applicative[F]]
            .pure(
              Left[ManagementServiceError, String](
                ManagementServiceError.NonExistentInstanceTypeError(
                  instanceTypeName
                )
              )
            )
        end if
      }
      _ <- traceT(
        s"Instance $result created with type name $instanceTypeName and values $values"
      )
    } yield result).value
  end create

  override def read(
      instanceId: String
  ): F[Either[ManagementServiceError, Entity]] =

    def fetchFieldsForInstance(
        instanceId: String
    ): F[List[(String, String)]] = {
      val query: String =
        s"""
           |PREFIX ns:  <${ns.getName}>
           |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
           |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
           |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
           |SELECT ?field ?value WHERE {
           | BIND(iri("${ns.getName}$instanceId") as ?entity)
           |    ?entity ?field ?value .
           |    FILTER ( ?value not in ( ns:Entity ))
           |  }
           |""".stripMargin

      val bindings = for {
        _ <- logger.trace(
          s"Evaluated query $query for retrieving instance $instanceId"
        )
        ibs <- repository.evaluateQuery(query)
      } yield ibs.toList

      val fieldsAndValues = bindings.map(bs =>
        bs.map(b =>
          val value = b.getBinding("value")
          val field: (String, String) = (
            iri(b.getBinding("field").getValue.stringValue()).getLocalName,
            value.getValue.stringValue()
          )
          field
        )
      )

      fieldsAndValues
    }

    def fetchEntityFieldsAndTypeName(instanceId: String) = {
      fetchFieldsForInstance(instanceId)
        .map(lp =>
          val entityTypeName: String = iri(
            lp.filter(p => p(0) === "isClassifiedBy").map(_(1)).toArray.apply(0)
          ).getLocalName
          val fields = lp.filter(p => p(0) =!= "isClassifiedBy")
          (entityTypeName, fields)
        )
    }

    def handlePrimitiveDataTypes(
        fieldName: String,
        dataType: DataType,
        fieldValue: Option[List[(String, String)]]
    ): F[Tuple] =
      val tuple = dataType match
        case StringType(mode) =>
          fieldValue match
            case Some(value) =>
              mode match
                case Required =>
                  fieldName -> value(0)(1)
                case Repeated =>
                  fieldName -> value.map(_(1)).reverse
                case Nullable =>
                  if value(0)(1) === "null" then
                    fieldName -> Option.empty[String]
                  else fieldName -> Some(value(0)(1))
            case None =>
              fieldName -> List[String]()
        case DateType(mode) =>
          fieldValue match
            case Some(value) =>
              mode match
                case Required =>
                  fieldName -> LocalDate.parse(value(0)(1))
                case Repeated =>
                  fieldName -> value
                    .map(date => LocalDate.parse(date(1)))
                    .reverse
                case Nullable =>
                  if value(0)(1) === "null" then
                    fieldName -> Option.empty[LocalDate]
                  else fieldName -> Some(LocalDate.parse(value(0)(1)))
            case None =>
              fieldName -> List[LocalDate]()
        case TimestampDataType(mode) =>
          fieldValue match
            case Some(value) =>
              mode match
                case Required =>
                  fieldName -> ZonedDateTime.parse(value(0)(1))
                case Repeated =>
                  fieldName -> value
                    .map(instant => ZonedDateTime.parse(instant(1)))
                    .reverse
                case Nullable =>
                  if value(0)(1) === "null" then
                    fieldName -> Option.empty[ZonedDateTime]
                  else fieldName -> Some(ZonedDateTime.parse(value(0)(1)))
            case None =>
              fieldName -> List[ZonedDateTime]()
        case DoubleType(mode) =>
          fieldValue match
            case Some(value) =>
              mode match
                case Required =>
                  fieldName -> value(0)(1).toDouble
                case Repeated =>
                  fieldName -> value.map(_(1).toDouble).reverse
                case Nullable =>
                  if value(0)(1) === "null" then
                    fieldName -> Option.empty[Double]
                  else fieldName -> Some(value(0)(1).toDouble)
            case None =>
              fieldName -> List[Double]()
        case FloatType(mode) =>
          fieldValue match
            case Some(value) =>
              mode match
                case Required =>
                  fieldName -> value(0)(1).toFloat
                case Repeated =>
                  fieldName -> value.map(_(1).toFloat).reverse
                case Nullable =>
                  if value(0)(1) === "null" then
                    fieldName -> Option.empty[Float]
                  else fieldName -> Some(value(0)(1).toFloat)
            case None =>
              fieldName -> List[Float]()
        case IntType(mode) =>
          fieldValue match
            case Some(value) =>
              mode match
                case Required =>
                  fieldName -> value(0)(1).toInt
                case Repeated =>
                  fieldName -> value.map(_(1).toInt).reverse
                case Nullable =>
                  if value(0)(1) === "null" then fieldName -> Option.empty[Int]
                  else fieldName -> Some(value(0)(1).toInt)
            case None =>
              fieldName -> List[Int]()
        case _ => EmptyTuple
      Applicative[F].pure(tuple)
    end handlePrimitiveDataTypes

    def handleStructDataType(
        fieldName: String,
        dataType: StructType,
        maybeFieldValue: Option[List[(String, String)]]
    ): F[Tuple] =
      def createTupleForStructDataType(
          dataType: StructType,
          fieldValue: (String, String)
      ): F[Tuple] =
        val nestedStructIRI: IRI = iri(fieldValue(1))
        val nestedStructFields: F[List[(String, String)]] =
          fetchFieldsForInstance(nestedStructIRI.getLocalName)
        nestedStructFields.flatMap(x => fieldsToTuple(x, dataType))
      end createTupleForStructDataType

      maybeFieldValue match
        case Some(fieldValue) =>
          dataType.mode match
            case Required =>
              val tuple =
                createTupleForStructDataType(dataType, fieldValue.head)
              tuple.map(t => fieldName -> t)
            case Repeated =>
              val listOfFTuples: List[F[Tuple]] =
                fieldValue.map(createTupleForStructDataType(dataType, _))
              val tuples: F[List[Tuple]] =
                Traverse[List].sequence(listOfFTuples)
              tuples.map(ts => fieldName -> ts)
            case Nullable =>
              if fieldValue(0)(1) === "null" then
                Applicative[F].pure(fieldName -> Option.empty[Tuple])
              else
                val tuple =
                  createTupleForStructDataType(dataType, fieldValue.head)
                tuple.map(t => fieldName -> Some(t))
        case None =>
          Applicative[F].pure(fieldName -> None)
    end handleStructDataType

    def fieldsToTuple(
        fields: List[(String, String)],
        struct: StructType
    ): F[Tuple] =
      val groupedFields: Map[String, List[(String, String)]] =
        fields.groupBy(_(0))
      val tuples: List[F[Tuple]] = struct.records
        .map(record =>
          val (fieldName, dataType) = record
          val fieldValue = groupedFields.get(fieldName)
          dataType match
            case _: StringType =>
              handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
            case _: IntType =>
              handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
            case _: DateType =>
              handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
            case _: TimestampDataType =>
              handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
            case _: DoubleType =>
              handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
            case _: FloatType =>
              handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
            case struct: StructType =>
              handleStructDataType(fieldName, struct, fieldValue)
            case _ => Applicative[F].pure(EmptyTuple)
        )
      Traverse[List].sequence(tuples).map(_.fold(EmptyTuple)(_ :* _))
    end fieldsToTuple

    (for {
      _ <- traceT(s"About to read instance with id: $instanceId")
      fe <- EitherT[
        F,
        ManagementServiceError,
        (String, List[(String, String)])
      ](
        summon[Functor[F]].map(fetchEntityFieldsAndTypeName(instanceId))(
          Right[ManagementServiceError, (String, List[(String, String)])] _
        )
      )
      _ <- traceT(s"Retrieved fields and type name $fe")
      entityType: EntityType <- EitherT(typeManagementService.read(fe(0)))
      _ <- traceT(s"Retrieved the entity type $entityType with name ${fe(0)} ")
      tuple <- EitherT[F, ManagementServiceError, Tuple](
        summon[Functor[F]].map(fieldsToTuple(fe(1), entityType.schema))(
          Right[ManagementServiceError, Tuple]
        )
      )
      _ <- traceT(s"Loaded the tuple $tuple")
      entity <- EitherT[F, ManagementServiceError, Entity](
        summon[Applicative[F]].pure(
          Right[ManagementServiceError, Entity](
            Entity(instanceId, entityType.name, tuple)
          )
        )
      )
      _ <- traceT(s"About to return the entity $entity")
    } yield entity).value
  end read

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.=="
    )
  )
  override def update(
      instanceId: String,
      values: Tuple
  ): F[Either[ManagementServiceError, String]] =
    val statementsToRemove: F[List[Statement]] = fetchStatementsForInstance(
      instanceId
    )
    for {
      _ <- logger.trace(s"About to remove the instance $instanceId")
      stmts <- statementsToRemove
      _ <- logger.trace(
        s"Statements for removing the previous version \n${stmts.mkString("\n")}"
      )
      instanceType <- summon[Applicative[F]].pure(
        iri(
          stmts
            .filter(_.getPredicate == NS.ISCLASSIFIEDBY)
            .head
            .getObject
            .stringValue()
        ).getLocalName
      )
      _ <- logger.trace(s"$instanceId is classified by $instanceType")
      res <- createInstanceNoCheck(instanceId, instanceType, values, stmts)
    } yield res
  end update

  override def delete(
      instanceId: String
  ): F[Either[ManagementServiceError, Unit]] =
    (for {
      _ <- logger.trace(s"About to remove the instance $instanceId")
      stmts <- fetchStatementsForInstance(instanceId)
      _ <- logger.trace(
        s"About to remove the statements \n${stmts.mkString("\n")}"
      )
      _ <- repository.removeAndInsertStatements(
        List.empty[Statement],
        stmts.map(st => (st.getSubject, st.getPredicate, st.getObject))
      )
    } yield ()).map(_ => Right[ManagementServiceError, Unit](()))
  end delete

  override def exist(
      instanceId: String
  ): F[Either[ManagementServiceError, Boolean]] =
    val res = repository.evaluateQuery(s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |SELECT ?entity WHERE {
         |    BIND(iri("${ns.getName}$instanceId") as ?entity)
         |  }
         |""".stripMargin)
    summon[Functor[F]].map(res)(res => {
      val count = res.toList.length
      if count > 0
      then Right[ManagementServiceError, Boolean](true)
      else Right[ManagementServiceError, Boolean](false)
    })
  end exist

  override def link(
      instanceId1: String,
      linkType: Relationship,
      instanceId2: String
  ): F[Either[ManagementServiceError, Unit]] =
    val query =
      s"""
       |PREFIX ns:  <${ns.getName}>
       |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
       |PREFIX owl: <http://www.w3.org/2002/07/owl#>
       |SELECT (COUNT(*) as ?count) WHERE {
       |        BIND(iri("${ns.getName}$instanceId1") as ?instanceId1)
       |        BIND(iri("${ns.getName}$instanceId2") as ?instanceId2)
       |        ?instanceId1 ns:isClassifiedBy ?type1 .
       |        ?instanceId2 ns:isClassifiedBy ?type2 .
       |        ?type1 ns:hasTrait ?trait1 .
       |        ?type2 ns:hasTrait ?trait2 .
       |        ?trait1 rdfs:subClassOf* ?tr1 .
       |        ?trait2 rdfs:subClassOf* ?tr2 .
       |        ?tr1 <${linkType.getNamespace}$linkType> ?tr2 .
       |  }
       |""".stripMargin
    val res =
      logger.trace(
        s"Querying about $instanceId1 and $instanceId2 if linked using $linkType"
      ) *> logger.trace(s"Using query $query") *> repository.evaluateQuery(
        query
      )

    val statements = List(
      statement(
        triple(
          iri(ns, instanceId1),
          iri(linkType.getNamespace, linkType),
          iri(ns, instanceId2)
        ),
        L3
      )
    )
    (for {
      _ <- traceT(
        s"About to link $instanceId1 with $instanceId2 using the relationship $linkType"
      )
      exist1 <- EitherT(exist(instanceId1))
      exist2 <- EitherT(exist(instanceId2))
      link <- EitherT(
        if exist1 && exist2 then
          summon[Functor[F]]
            .map(res)(ibs =>
              val count = ibs
                .map(bs => bs.getBinding("count").getValue.stringValue())
                .toList
                .headOption
              val found = count === Some("1")
              if found then
                repository
                  .removeAndInsertStatements(statements, List.empty)
                  .map(_ => Right[ManagementServiceError, Unit](()))
              else
                summon[Applicative[F]].pure(
                  Left[ManagementServiceError, Unit](
                    InvalidLinkType(instanceId1, linkType, instanceId2)
                  )
                )
              end if
            )
            .flatten
        else
          if !exist1 then
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Unit](
                NonExistentInstanceError(instanceId1)
              )
            )
          else if !exist2 then
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Unit](
                NonExistentInstanceError(instanceId2)
              )
            )
          else
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Unit](
                NonExistentInstanceError(instanceId1)
              )
            )
          end if
      )
    } yield link).value
  end link

  override def unlink(
      instanceId1: String,
      linkType: Relationship,
      instanceId2: String
  ): F[Either[ManagementServiceError, Unit]] =
    val statements = List(
      statement(
        triple(
          iri(ns, instanceId1),
          iri(linkType.getNamespace, linkType),
          iri(ns, instanceId2)
        ),
        L3
      )
    )
    (for {
      _ <- traceT(
        s"About to unlink $instanceId1 with $instanceId2 using the relationship $linkType"
      )
      exist1 <- EitherT(exist(instanceId1))
      exist2 <- EitherT(exist(instanceId2))
      res <- EitherT(
        if exist1 && exist2 then
          summon[Functor[F]].map(
            repository.removeAndInsertStatements(
              List.empty,
              statements.map(s => (s.getSubject, s.getPredicate, s.getObject))
            )
          )(_ => Right[ManagementServiceError, Unit](()))
        else
          if !exist1 then
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Unit](
                NonExistentInstanceError(instanceId1)
              )
            )
          else
            if !exist2 then
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  NonExistentInstanceError(instanceId2)
                )
              )
            else
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  NonExistentInstanceError(instanceId1)
                )
              )
            end if
          end if
      )
    } yield res).value
  end unlink

  override def linked(
      instanceId: String,
      linkType: Relationship
  ): F[Either[ManagementServiceError, List[String]]] =
    val query =
      s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |SELECT ?linked WHERE {
         |  BIND(iri("${ns.getName}$instanceId") as ?instance)
         |    ?instance <${linkType.getNamespace}$linkType> ?linked .
         |  }
         |""".stripMargin

    (for {
      _ <- traceT(
        s"Looking for linked instances for instance $instanceId and relationship kind $linkType"
      )
      exist <- EitherT(exist(instanceId))
      _ <- traceT(s"Looking for linked instances with the query: $query")
      res <- EitherT(
        if exist then
          summon[Functor[F]].map(
            repository
              .evaluateQuery(query)
              .map(
                _.map(bs =>
                  iri(
                    bs.getBinding("linked").getValue.stringValue()
                  ).getLocalName
                ).toList
              )
          )(Right[ManagementServiceError, List[String]])
        else
          summon[Applicative[F]].pure(
            Left[ManagementServiceError, List[String]](
              NonExistentInstanceError(instanceId)
            )
          )
      )
    } yield res).value
  end linked

end InstanceManagementServiceInterpreter
