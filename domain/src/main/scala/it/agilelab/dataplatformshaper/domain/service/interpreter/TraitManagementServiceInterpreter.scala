package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.data.EitherT
import cats.effect.Sync
import cats.implicits.*
import cats.{Applicative, Functor}
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.NS
import it.agilelab.dataplatformshaper.domain.model.NS.{L1, ns}
import it.agilelab.dataplatformshaper.domain.model.l1.{*, given}
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.*
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
      traitName: String,
      inheritsFrom: Option[String]
  ): F[Either[ManagementServiceError, Unit]] =
    val traitIri = iri(ns, traitName)

    val statements = inheritsFrom.fold(
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
        s"About to create a trait with name $traitName"
      )
      exist <- EitherT(exist(traitName))
      _ <- traceT(
        s"Checking the existence, does $traitName already exist? $exist"
      )
      stmts <- EitherT(
        summon[Applicative[F]].pure(
          Right[ManagementServiceError, List[Statement]](statements)
        )
      )
      _ <- EitherT {
        if !exist
        then
          summon[Functor[F]].map(
            repository.removeAndInsertStatements(
              stmts
            )
          )(Right[ManagementServiceError, Unit])
        else
          summon[Applicative[F]]
            .pure(
              Left[ManagementServiceError, Unit](
                ManagementServiceError(
                  s"The trait $traitName has been already defined"
                )
              )
            )
      }
      _ <- traceT(s"Instance type created")
    } yield ()).value
  end create

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
    summon[Functor[F]].map(res)(res => {
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

    summon[Functor[F]].map(res) { bindings =>
      val existingTraits = bindings
        .flatMap(binding =>
          Option(binding.getValue("trait")).map(_.stringValue())
        )
        .toSet
      val result = traitNames.map(traitName =>
        (traitName, existingTraits.contains(iri(ns, traitName).stringValue()))
      )
      Right[ManagementServiceError, Set[(String, Boolean)]](result)
    }
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
          summon[Functor[F]].map(
            repository.removeAndInsertStatements(statements, List.empty)
          )(_ => Right[ManagementServiceError, Unit](()))
        else
          if !exist1
          then
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Unit](
                ManagementServiceError(s"The trait $traitName1 does not exist")
              )
            )
          else
            if !exist2
            then
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  ManagementServiceError(
                    s"The trait $traitName2 does not exist"
                  )
                )
              )
            else
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  ManagementServiceError(
                    s"The trait $traitName1 $traitName2 or does not exist"
                  )
                )
              )
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
          summon[Functor[F]]
            .map(res)(ibs =>
              val count = ibs
                .map(bs => bs.getBinding("count").getValue.stringValue().toInt)
                .toList
                .head
              if count === 0 then
                summon[Functor[F]].map(
                  repository.removeAndInsertStatements(
                    List.empty,
                    statements
                  )
                )(_ => Right[ManagementServiceError, Unit](()))
              else
                summon[Applicative[F]].pure(
                  Left[ManagementServiceError, Unit](
                    ManagementServiceError(
                      s"Traits $traitName1 and $traitName2 have linked instances"
                    )
                  )
                )
              end if
            )
            .flatten
        else
          if !exist1
          then
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Unit](
                ManagementServiceError(s"The trait $traitName1 does not exist")
              )
            )
          else
            if !exist2
            then
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  ManagementServiceError(
                    s"The trait $traitName2 does not exist"
                  )
                )
              )
            else
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  ManagementServiceError(
                    s"The trait $traitName1 or $traitName2 does not exist"
                  )
                )
              )
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
              ManagementServiceError(s"The trait $traitName does not exist")
            )
          )
      )
    } yield res).value
  end linked

end TraitManagementServiceInterpreter
