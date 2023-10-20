package it.agilelab.dataplatformshaper.domain.service

enum ManagementServiceError(errorMessage: String):
  def getMessage: String = errorMessage
  case TypeAlreadyDefinedError(instanceTypeName: String)
      extends ManagementServiceError(
        s"The type $instanceTypeName has been already defined"
      )
  case TraitAlreadyDefinedError(traitName: String)
      extends ManagementServiceError(
        s"The trait $traitName has been already defined"
      )
  case NonExistentInstanceTypeError(instanceTypeName: String)
      extends ManagementServiceError(
        s"The instance type with name $instanceTypeName does not exist"
      )
  case NonExistentInstanceError(instanceId: String)
      extends ManagementServiceError(
        s"The instance with id $instanceId does not exist"
      )
  case TupleIsNotConformToSchema(parsingError: String)
      extends ManagementServiceError(parsingError)
  case InvalidLinkType(inst1: String, linkType: String, inst2: String)
      extends ManagementServiceError(
        s"Invalide link type $linkType between instance $inst1 and $inst2"
      )
