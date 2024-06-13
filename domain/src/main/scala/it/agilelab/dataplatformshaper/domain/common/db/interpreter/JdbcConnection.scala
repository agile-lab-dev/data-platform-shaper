package it.agilelab.dataplatformshaper.domain.common.db.interpreter

import it.agilelab.dataplatformshaper.domain.common.db.Connection as dbConnection

import java.sql.Connection

case class JdbcConnection(connection: Connection) extends dbConnection
end JdbcConnection
