package it.agilelab.dataplatformshaper.domain.service

final case class ManagementServiceError(errors: List[String])

object ManagementServiceError:
  def apply(error: String): ManagementServiceError = ManagementServiceError(
    List(error)
  )
end ManagementServiceError
