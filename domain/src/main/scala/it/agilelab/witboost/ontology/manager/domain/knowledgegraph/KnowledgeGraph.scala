package it.agilelab.witboost.ontology.manager.domain.knowledgegraph

import org.eclipse.rdf4j.model.{IRI, Resource, Statement, Value}
import org.eclipse.rdf4j.query.BindingSet

trait KnowledgeGraph[F[_]]:
  def removeAndInsertStatements(
      statement: List[Statement],
      deleteStatements: List[(Resource, IRI, Value)] =
        List.empty[(Resource, IRI, Value)]
  ): F[Unit]
  def evaluateQuery(query: String): F[Iterator[BindingSet]]
end KnowledgeGraph
