package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.data.EitherT
import cats.effect.Sync
import cats.{Applicative, Functor}
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.NS
import it.agilelab.dataplatformshaper.domain.model.NS.{L1, ns}
import it.agilelab.dataplatformshaper.domain.model.l1.Relationship
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

  override def link(
      trait1Name: String,
      linkType: Relationship,
      traitName2: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def unlink(
      trait1Name: String,
      linkType: Relationship,
      trait2Name: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def linked(
      traitName: String,
      linkType: Relationship
  ): F[Either[ManagementServiceError, List[String]]] = ???

end TraitManagementServiceInterpreter
