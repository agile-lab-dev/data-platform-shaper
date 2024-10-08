package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.data.EitherT
import cats.effect.Sync
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.NS.{L1, ns}
import it.agilelab.dataplatformshaper.domain.model.{*, given}
import it.agilelab.dataplatformshaper.domain.service.{
  ManagementServiceError,
  TraitManagementService
}
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.model.util.Statements.statement
import org.eclipse.rdf4j.model.util.Values.{iri, triple}
import org.eclipse.rdf4j.model.vocabulary.{OWL, RDF, RDFS}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class TraitManagementServiceInterpreter[F[_]: Sync](
  val repository: KnowledgeGraph[F]
) extends TraitManagementService[F]:

  given logger: Logger[F] = Slf4jLogger.getLogger[F]

  override def create(
    traitDefinition: Trait
  ): F[Either[ManagementServiceError, Unit]] =
    val traitIri = iri(ns, traitDefinition.traitName)

    val statements = traitDefinition.inheritsFrom.fold(
      List(
        statement(triple(traitIri, RDF.TYPE, OWL.NAMEDINDIVIDUAL), L1),
        statement(triple(traitIri, RDF.TYPE, NS.TRAIT), L1)
      )
    )(inhf =>
      List(
        statement(triple(traitIri, RDF.TYPE, OWL.NAMEDINDIVIDUAL), L1),
        statement(triple(traitIri, RDF.TYPE, NS.TRAIT), L1),
        statement(triple(traitIri, RDFS.SUBCLASSOF, iri(ns, inhf)), L1)
      )
    )

    (for {
      _ <- traceT(
        s"About to create a trait with name ${traitDefinition.traitName}"
      )
      exist <- EitherT(exist(traitDefinition.traitName))
      _ <- traceT(
        s"Checking the existence, does ${traitDefinition.traitName} already exist? $exist"
      )
      fatherExist <- EitherT(
        traitDefinition.inheritsFrom.fold(
          Right[ManagementServiceError, Boolean](true).pure[F]
        )(inhf => this.exist(inhf))
      )
      _ <- traceT(s"Checking if the father exists: $fatherExist")
      stmts <- EitherT(
        Right[ManagementServiceError, List[Statement]](statements).pure[F]
      )
      _ <- EitherT {
        if fatherExist then
          if !exist
          then
            repository
              .removeAndInsertStatements(stmts)
              .map(Right[ManagementServiceError, Unit])
          else
            Left[ManagementServiceError, Unit](
              ManagementServiceError(
                s"The trait ${traitDefinition.traitName} has been already defined"
              )
            ).pure[F]
        else
          Left[ManagementServiceError, Unit](
            ManagementServiceError(
              s"The father trait ${traitDefinition.inheritsFrom.getOrElse("")} does not exist"
            )
          ).pure[F]
        end if
      }
      _ <- traceT(s"Instance type created")
    } yield ()).value
  end create

  def create(
    bulkTraitsCreationRequest: BulkTraitsCreationRequest
  ): F[BulkTraitsCreationResponse] =

    val x: F[List[(Trait, Option[String])]] = bulkTraitsCreationRequest.traits
      .map(traitDefinition =>
        this
          .create(traitDefinition)
          .map((traitDefinition, _))
      )
      .sequence
      .map(_.map {
        case (traitDefinition, Left(error)) =>
          (traitDefinition, Some(error.errors.mkString(",")))
        case (traitDefinition, Right(_)) =>
          (traitDefinition, None)
      })

    val y: F[List[((String, Relationship, String), Option[String])]] =
      bulkTraitsCreationRequest.relationships
        .map(rel =>
          this
            .link(rel(0), rel(1), rel(2))
            .map(((rel(0), rel(1): Relationship, rel(2)), _))
        )
        .sequence
        .map(_.map {
          case (linkDefinition, Left(error)) =>
            (linkDefinition, Some(error.errors.mkString(",")))
          case (linkDefinition, Right(_)) =>
            (linkDefinition, None)
        })

    for {
      xr <- x
      yr <- y
    } yield BulkTraitsCreationResponse(xr, yr)
  end create

  override def delete(
    traitName: String
  ): F[Either[ManagementServiceError, Unit]] =
    val traitIri = iri(ns, traitName)
    val initialStatements: List[Statement] = List(
      statement(triple(traitIri, RDF.TYPE, OWL.NAMEDINDIVIDUAL), L1),
      statement(triple(traitIri, RDF.TYPE, NS.TRAIT), L1)
    )
    (for {
      _ <-
        if traitName.equals("MappingSource") || traitName.equals(
            "MappingTarget"
          )
        then
          EitherT.leftT[F, Unit](
            ManagementServiceError(s"Cannot delete the trait $traitName")
          )
        else EitherT.rightT[F, ManagementServiceError](())
      traitExists <- EitherT(exist(traitName))
      _ <-
        if !traitExists then
          EitherT.leftT[F, Unit](
            ManagementServiceError(s"The trait $traitName does not exist")
          )
        else EitherT.rightT[F, ManagementServiceError](())
      hasEntities <- EitherT(entityHasTrait(traitName))
      _ <-
        if hasEntities then
          EitherT.leftT[F, Unit](
            ManagementServiceError(s"An entity has the trait $traitName")
          )
        else EitherT.rightT[F, ManagementServiceError](())
      hasLinks <- EitherT(hasLinkedTraits(traitName))
      _ <-
        if hasLinks then
          EitherT.leftT[F, Unit](
            ManagementServiceError(
              s"The trait $traitName is linked to another trait (inheritance or linking)"
            )
          )
        else EitherT.rightT[F, ManagementServiceError](())
      superClass <- EitherT.liftF(getSuperClassOf(traitName))
      additionalStatements <- EitherT.liftF(
        superClass
          .map(trait2 =>
            List(
              statement(triple(traitIri, RDFS.SUBCLASSOF, iri(ns, trait2)), L1)
            )
          )
          .getOrElse(List.empty[Statement])
          .pure[F]
      )
      statementsToRemove = initialStatements ::: additionalStatements
      _ <- EitherT.liftF(
        repository
          .removeAndInsertStatements(List.empty[Statement], statementsToRemove)
      )
    } yield ()).value
  end delete

  private def hasLinkedTraits(
    traitName: String
  ): F[Either[ManagementServiceError, Boolean]] =
    val traitIri = iri(ns, traitName)

    val query = s"""
        |PREFIX ns:  <${ns.getName}>
        |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        |PREFIX owl: <http://www.w3.org/2002/07/owl#>
        |SELECT (COUNT(DISTINCT *) AS ?count) WHERE {
        |     ?trait1 ?predicate ?trait2 .
        |     ?trait1 rdf:type owl:NamedIndividual .
        |     ?trait1 rdf:type ns:Trait .
        |     ?trait2 rdf:type owl:NamedIndividual .
        |     ?trait2 rdf:type ns:Trait .
        |     FILTER(?trait1 = <$traitIri> || ?trait2 = <$traitIri>)
        |     FILTER(?trait1 != <$traitIri> || ?predicate != rdfs:subClassOf)
        |}
        |""".stripMargin
    (for {
      _ <- traceT(s"Looking for linked traits for trait $traitName")
      queryResult = repository.evaluateQuery(query)
      res <- EitherT(queryResult.map(resultSet =>
        val resultList = resultSet.toList
        Right(
          resultList.nonEmpty && resultList.head
            .getValue("count")
            .stringValue()
            .toInt > 0
        )
      ))
    } yield res).value
  end hasLinkedTraits

  private def entityHasTrait(
    traitName: String
  ): F[Either[ManagementServiceError, Boolean]] =
    val traitIri = iri(ns, traitName)
    val query = s"""
                   |PREFIX ns:  <${ns.getName}>
                   |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                   |PREFIX owl: <http://www.w3.org/2002/07/owl#>
                   |SELECT (COUNT(DISTINCT *) AS ?count) WHERE {
                   |     ?entityType ?predicate ?trait .
                   |     ?trait rdf:type owl:NamedIndividual .
                   |     ?trait rdf:type ns:Trait .
                   |     ?entityType rdf:type ns:EntityType .
                   |     FILTER(?trait = <$traitIri>)
                   |}
                   |""".stripMargin
    (for {
      _ <- traceT(s"Looking for entities having trait $traitName")
      queryResult = repository.evaluateQuery(query)
      res <- EitherT(queryResult.map(resultSet =>
        val resultList = resultSet.toList
        Right(
          resultList.nonEmpty && resultList.head
            .getValue("count")
            .stringValue()
            .toInt > 0
        )
      ))
    } yield res).value
  end entityHasTrait

  private def getSuperClassOf(traitName: String): F[Option[String]] =
    val traitIri = iri(ns.getName, traitName)
    val query = s"""
        |PREFIX ns:  <${ns.getName}>
        |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        |PREFIX owl: <http://www.w3.org/2002/07/owl#>
        |SELECT ?trait2 WHERE {
        |   ?trait1 ?predicate ?trait2 .
        |   ?trait1 rdf:type owl:NamedIndividual .
        |   ?trait1 rdf:type ns:Trait .
        |   ?trait2 rdf:type owl:NamedIndividual .
        |   ?trait2 rdf:type ns:Trait .
        |   FILTER(?predicate = rdfs:subClassOf && ?trait1 = <$traitIri>)
        |}
        |""".stripMargin

    val queryResult = repository.evaluateQuery(query)
    queryResult.map(
      _.nextOption().map(bs =>
        iri(bs.getValue("trait2").stringValue()).getLocalName
      )
    )
  end getSuperClassOf

  override def exist(
    traitName: String
  ): F[Either[ManagementServiceError, Boolean]] =
    val traitIri = iri(ns, traitName)
    val res = repository.evaluateQuery(s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |SELECT ?trait WHERE {
         |   BIND(iri("${traitIri.stringValue()}") as ?trait)
         |     ?trait rdf:type owl:NamedIndividual .
         |     ?trait rdf:type ns:Trait .
         |}
         |""".stripMargin)
    res.map(res => {
      val count = res.toList.length
      if count > 0
      then Right[ManagementServiceError, Boolean](true)
      else Right[ManagementServiceError, Boolean](false)
    })
  end exist

  override def exist(
    traitNames: Set[String]
  ): F[Either[ManagementServiceError, Set[(String, Boolean)]]] =

    val traitIris = traitNames
      .map(traitName => s"""<${iri(ns, traitName).stringValue()}>""")
      .mkString(" ")

    val query =
      s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |SELECT ?trait WHERE {
         |  VALUES ?trait { $traitIris }
         |  ?trait rdf:type owl:NamedIndividual .
         |  ?trait rdf:type ns:Trait .
         |}
         |""".stripMargin

    val res = repository.evaluateQuery(query)

    res.map(bindings =>
      val existingTraits = bindings
        .flatMap(binding =>
          Option(binding.getValue("trait")).map(_.stringValue())
        )
        .toSet
      val result = traitNames.map(traitName =>
        (traitName, existingTraits.contains(iri(ns, traitName).stringValue()))
      )
      Right[ManagementServiceError, Set[(String, Boolean)]](result)
    )
  end exist

  override def link(
    traitName1: String,
    linkType: Relationship,
    traitName2: String
  ): F[Either[ManagementServiceError, Unit]] =
    val statements = List(
      statement(
        triple(
          iri(ns, traitName1),
          iri(linkType.getNamespace, linkType),
          iri(ns, traitName2)
        ),
        L1
      )
    ) ::: linkType.getInverse.fold(Nil: List[Statement])(inverse =>
      List(
        statement(
          triple(
            iri(ns, traitName2),
            iri(linkType.getNamespace, inverse),
            iri(ns, traitName1)
          ),
          L1
        )
      )
    )

    (for {
      _ <- traceT(
        s"About to link $traitName1 with $traitName2 using the relationship $linkType"
      )
      exist1 <- EitherT(exist(traitName1))
      exist2 <- EitherT(exist(traitName2))
      link <- EitherT(
        if exist1 && exist2
        then
          repository
            .removeAndInsertStatements(statements, List.empty)
            .map(_ => Right[ManagementServiceError, Unit](()))
        else
          if !exist1
          then
            Left[ManagementServiceError, Unit](
              ManagementServiceError(s"The trait $traitName1 does not exist")
            ).pure[F]
          else
            if !exist2
            then
              Left[ManagementServiceError, Unit](
                ManagementServiceError(s"The trait $traitName2 does not exist")
              ).pure[F]
            else
              Left[ManagementServiceError, Unit](
                ManagementServiceError(
                  s"The trait $traitName1 $traitName2 or does not exist"
                )
              ).pure[F]
            end if
          end if
      )
    } yield link).value
  end link

  override def unlink(
    traitName1: String,
    linkType: Relationship,
    traitName2: String
  ): F[Either[ManagementServiceError, Unit]] =
    val query =
      s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |PREFIX owl:  <http://www.w3.org/2002/07/owl#>
         |SELECT  (count(*) as ?count) WHERE {
         |        BIND(iri("${ns.getName}$traitName1") as ?trait1)
         |        BIND(iri("${ns.getName}$traitName2") as ?trait2)
         |        ?tr1 <${linkType.getNamespace}$linkType>  ?tr2 .
         |        ?instance1 <${linkType.getNamespace}$linkType> ?instance2 .
         |        ?instance1 ns:isClassifiedBy ?type1 .
         |        ?instance2 ns:isClassifiedBy ?type2 .
         |        ?type1 ns:hasTrait ?trait1 .
         |        ?type2 ns:hasTrait ?trait2 .
         |        ?trait1 rdfs:subClassOf* ?tr1 .
         |        ?trait2 rdfs:subClassOf* ?tr2 .
         |  }
         |""".stripMargin
    val res =
      logger.trace(
        s"Querying if there are linked instances using using this $linkType associated with traits $traitName1 and $traitName2"
      ) *> logger.trace(s"Using query $query") *> repository.evaluateQuery(
        query
      )

    val statements = List(
      statement(
        triple(
          iri(ns, traitName1),
          iri(linkType.getNamespace, linkType),
          iri(ns, traitName2)
        ),
        L1
      )
    ) ::: linkType.getInverse.fold(Nil: List[Statement])(inverse =>
      List(
        statement(
          triple(
            iri(ns, traitName2),
            iri(linkType.getNamespace, inverse),
            iri(ns, traitName1)
          ),
          L1
        )
      )
    )

    (for {
      _ <- traceT(
        s"About to unlink $traitName1 with $traitName2 using the relationship $linkType"
      )
      exist1 <- EitherT(exist(traitName1))
      exist2 <- EitherT(exist(traitName2))
      res <- EitherT(
        if exist1 && exist2
        then
          res
            .map(ibs =>
              val count = ibs
                .map(bs => bs.getBinding("count").getValue.stringValue().toInt)
                .toList
                .head
              if count === 0 then
                repository
                  .removeAndInsertStatements(List.empty, statements)
                  .map(_ => Right[ManagementServiceError, Unit](()))
              else
                Left[ManagementServiceError, Unit](
                  ManagementServiceError(
                    s"Traits $traitName1 and $traitName2 have linked instances"
                  )
                ).pure[F]
              end if
            )
            .flatten
        else
          if !exist1
          then
            Left[ManagementServiceError, Unit](
              ManagementServiceError(s"The trait $traitName1 does not exist")
            ).pure[F]
          else
            if !exist2
            then
              Left[ManagementServiceError, Unit](
                ManagementServiceError(s"The trait $traitName2 does not exist")
              ).pure[F]
            else
              Left[ManagementServiceError, Unit](
                ManagementServiceError(
                  s"The trait $traitName1 or $traitName2 does not exist"
                )
              ).pure[F]
            end if
          end if
      )
    } yield res).value
  end unlink

  override def linked(
    traitName: String,
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
         |  BIND(iri("${ns.getName}$traitName") as ?trait)
         |    ?trait <${linkType.getNamespace}$linkType> ?linked .
         |  }
         |""".stripMargin

    (for {
      _ <- traceT(
        s"Looking for linked traits for trait $traitName and relationship kind $linkType"
      )
      exist <- EitherT(exist(traitName))
      _ <- traceT(s"Looking for linked traits with the query: $query")
      res <- EitherT(
        if exist then
          repository
            .evaluateQuery(query)
            .map(
              _.map(bs =>
                iri(bs.getBinding("linked").getValue.stringValue()).getLocalName
              ).toList
            )
            .map(Right[ManagementServiceError, List[String]])
        else
          Left[ManagementServiceError, List[String]](
            ManagementServiceError(s"The trait $traitName does not exist")
          ).pure[F]
      )
    } yield res).value
  end linked

  override def list(): F[Either[ManagementServiceError, List[String]]] =
    val query: String =
      s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |SELECT DISTINCT ?traitName WHERE {
         |  ?traitName ?predicate ?object .
         |  ?traitName rdf:type ns:Trait
         | }
         |""".stripMargin
    for
      queryResults <- repository.evaluateQuery(query)
      traitNames = Right(
        queryResults
          .map(bs =>
            iri(bs.getBinding("traitName").getValue.stringValue()).getLocalName
          )
          .toList
      )
    yield traitNames
  end list

end TraitManagementServiceInterpreter
