package it.agilelab.dataplatformshaper.domain.common.db.interpreter

import cats.effect.Sync
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.NS.{L0, ns}
import org.eclipse.rdf4j.model.util.Values.iri
import org.eclipse.rdf4j.model.{Resource, Statement}
import org.eclipse.rdf4j.query.BindingSet
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import scala.jdk.CollectionConverters.*

class Rdf4jKnowledgeGraph[F[_]: Sync](session: Rdf4jSession)
    extends KnowledgeGraph[F]:

  override def removeAndInsertStatements(
    statements: List[Statement],
    deleteStatements: List[Statement]
  ): F[Unit] =
    session.withTx(conn => {
      deleteStatements.foreach(st => {
        conn.remove(st.getSubject, st.getPredicate, st.getObject, st.getContext)
      })
      conn.add(statements.asJava)
    })
  end removeAndInsertStatements

  override def evaluateQuery(query: String): F[Iterator[BindingSet]] =
    session.withTx { connection =>
      val tupledQuery = connection.prepareTupleQuery(query)
      tupledQuery.evaluate().iterator().asScala
    }
  end evaluateQuery

  @SuppressWarnings(Array("scalafix:DisableSyntax.null"))
  override def loadBaseOntologies(): F[Unit] =
    val repository = Rdf4jKnowledgeGraph[F](session)
    val model = Rio.parse(
      Thread.currentThread.getContextClassLoader
        .getResourceAsStream("dp-ontology.owl"),
      ns.getName,
      RDFFormat.RDFXML,
      L0
    )
    val statements = model.getStatements(null, null, null, iri(ns, "L0"))
    repository.removeAndInsertStatements(statements.asScala.toList)
  end loadBaseOntologies
end Rdf4jKnowledgeGraph
