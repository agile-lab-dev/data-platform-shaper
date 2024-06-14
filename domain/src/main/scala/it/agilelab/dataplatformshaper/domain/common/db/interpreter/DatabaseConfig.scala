package it.agilelab.dataplatformshaper.domain.common.db.interpreter

case class DatabaseConfig(
  url: String,
  user: Option[String],
  password: Option[Array[Char]],
  migrationsTable: String,
  migrationsLocations: List[String]
)
