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
                ManagementServiceError.TraitAlreadyDefinedError(traitName)
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
    ???
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
                NonExistentTraitError(traitName1)
              )
            )
          else
            if !exist2
            then
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  NonExistentTraitError(traitName2)
                )
              )
            else
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  NonExistentTraitError(traitName1)
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
          summon[Functor[F]].map(
            repository.removeAndInsertStatements(
              List.empty,
              statements.map(s => (s.getSubject, s.getPredicate, s.getObject))
            )
          )(_ => Right[ManagementServiceError, Unit](()))
        else
          if !exist1
          then
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Unit](
                NonExistentTraitError(traitName1)
              )
            )
          else
            if !exist2
            then
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  NonExistentTraitError(traitName2)
                )
              )
            else
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  NonExistentTraitError(traitName1)
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
              NonExistentTraitError(traitName)
            )
          )
      )
    } yield res).value
  end linked

end TraitManagementServiceInterpreter
