package it.agilelab.dataplatformshaper.domain.common.db.interpreter

import it.agilelab.dataplatformshaper.domain.common.db.Connection as dbConnection
import org.eclipse.rdf4j.repository.RepositoryConnection

case class Rdf4jConnection(connection: RepositoryConnection) extends dbConnection
end Rdf4jConnection
