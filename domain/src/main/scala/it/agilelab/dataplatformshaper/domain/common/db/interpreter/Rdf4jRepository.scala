package it.agilelab.dataplatformshaper.domain.common.db.interpreter

import cats.effect.Sync
import it.agilelab.dataplatformshaper.domain.common.db.{
  Connection,
  KnowledgeGraph
}
import it.agilelab.dataplatformshaper.domain.model.NS.{L0, ns}
import org.eclipse.rdf4j.model.util.Values.iri
import org.eclipse.rdf4j.model.{Resource, Statement}
import org.eclipse.rdf4j.query.BindingSet
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import java.security.InvalidParameterException
import scala.jdk.CollectionConverters.*

case class Rdf4jRepository[F[_]: Sync](session: Rdf4jSession)
    extends KnowledgeGraph[F]:

  private def connectionToRdf4jConnection(conn: Connection): Rdf4jConnection =
    conn match
      case rdfConn: Rdf4jConnection => rdfConn
      case _ =>
        throw new InvalidParameterException(
          "Expected the connection to be an Rdf4jConnection"
        )
  end connectionToRdf4jConnection

  def removeAndInsertStatements(
    statements: List[Statement],
    deleteStatements: List[Statement]
  ): F[Unit] =
    session.withTx(conn => {
      val connection = connectionToRdf4jConnection(conn).connection
      deleteStatements.foreach(st => {
        connection.remove(
          st.getSubject,
          st.getPredicate,
          st.getObject,
          st.getContext
        )
      })
      connection.add(statements.asJava)
    })
  end removeAndInsertStatements

  def evaluateQuery(query: String): F[Iterator[BindingSet]] =
    session.withTx { conn =>
      val connection = connectionToRdf4jConnection(conn).connection
      val tupledQuery = connection.prepareTupleQuery(query)
      tupledQuery.evaluate().iterator().asScala
    }
  end evaluateQuery

  @SuppressWarnings(Array("scalafix:DisableSyntax.null"))
  def loadBaseOntologies(): F[Unit] =
    val repository = Rdf4jRepository[F](session)
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

end Rdf4jRepository
