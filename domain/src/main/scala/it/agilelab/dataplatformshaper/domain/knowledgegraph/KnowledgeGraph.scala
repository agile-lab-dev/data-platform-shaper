package it.agilelab.dataplatformshaper.domain.knowledgegraph

import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.query.BindingSet

trait KnowledgeGraph[F[_]]:
  def removeAndInsertStatements(
      statement: List[Statement],
      deleteStatements: List[Statement] = List.empty[Statement]
  ): F[Unit]
  def evaluateQuery(query: String): F[Iterator[BindingSet]]
end KnowledgeGraph
