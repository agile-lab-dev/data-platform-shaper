package it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter

import cats.effect.Sync
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.NS.{L0, L1, ns}
import org.eclipse.rdf4j.model.util.Values.iri
import org.eclipse.rdf4j.model.{Resource, Statement}
import org.eclipse.rdf4j.query.BindingSet
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import scala.jdk.CollectionConverters.*

class Rdf4jKnowledgeGraph[F[_]: Sync](session: Session)
    extends KnowledgeGraph[F]:

  override def removeAndInsertStatements(
      statements: List[Statement],
      deleteStatements: List[Statement]
  ): F[Unit] =
    session.withTx(conn => {
      deleteStatements.foreach(st => {
        conn.remove(
          st.getSubject,
          st.getPredicate,
          st.getObject,
          st.getContext
        )
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

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.null"
    )
  )
  override def loadBaseOntologies(): F[Unit] =
    val repository = Rdf4jKnowledgeGraph[F](session)
    val model1 = Rio.parse(
      Thread.currentThread.getContextClassLoader
        .getResourceAsStream("dp-ontology-l0.ttl"),
      ns.getName,
      RDFFormat.TURTLE,
      L0
    )
    val model2 = Rio.parse(
      Thread.currentThread.getContextClassLoader
        .getResourceAsStream("dp-ontology-l1.ttl"),
      ns.getName,
      RDFFormat.TURTLE,
      L1
    )
    val statements1 = model1.getStatements(null, null, null, iri(ns, "L0"))
    val statements2 = model2.getStatements(null, null, null, iri(ns, "L1"))
    repository.removeAndInsertStatements(
      statements1.asScala.toList ++ statements2.asScala.toList
    )
  end loadBaseOntologies
end Rdf4jKnowledgeGraph
