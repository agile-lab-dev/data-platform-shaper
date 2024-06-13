package it.agilelab.dataplatformshaper.domain.common.db

import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.query.BindingSet

trait KnowledgeGraph[F[_]] extends Repository[F]:

  def loadBaseOntologies(): F[Unit]

  def removeAndInsertStatements(
    statements: List[Statement],
    deleteStatements: List[Statement] = List.empty[Statement]
  ): F[Unit]

  def evaluateQuery(query: String): F[Iterator[BindingSet]]
end KnowledgeGraph
