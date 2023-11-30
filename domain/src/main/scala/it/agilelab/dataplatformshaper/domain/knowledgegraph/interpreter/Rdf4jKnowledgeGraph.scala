package it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter

import cats.effect.Sync
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import org.eclipse.rdf4j.model.{Resource, Statement}
import org.eclipse.rdf4j.query.BindingSet

import scala.jdk.CollectionConverters.*

class Rdf4jKnowledgeGraph[F[_]: Sync](session: Session)
    extends KnowledgeGraph[F]:
  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.defaultArgs"
    )
  )
  override def removeAndInsertStatements(
      statements: List[Statement],
      deleteStatements: List[Statement] = List.empty[Statement]
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
end Rdf4jKnowledgeGraph
