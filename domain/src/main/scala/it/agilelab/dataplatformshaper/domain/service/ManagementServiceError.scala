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
  case NonExistentTraitError(traitName: String)
      extends ManagementServiceError(
        s"The trait with name $traitName does not exist"
      )
  case TupleIsNotConformToSchema(parsingError: String)
      extends ManagementServiceError(parsingError)
  case InvalidLinkType(inst1: String, linkType: String, inst2: String)
      extends ManagementServiceError(
        s"Invalid link type $linkType between instance $inst1 and $inst2"
      )
  case InstanceHasLinkedInstancesError(inst: String)
      extends ManagementServiceError(s"The instance $inst has linked instances")
  case TypeHasInstancesError(entityType: String)
      extends ManagementServiceError(s"The type $entityType has instances")
  case TypeIsFatherError(entityType: String)
      extends ManagementServiceError(
        s"The type $entityType is the father of other instances"
      )
  case TraitsHaveLinkedInstancesError(traitName1: String, traitName2: String)
      extends ManagementServiceError(
        s"The traits $traitName1 and $traitName2 have associated linked instances"
      )
  case InvalidSearchPredicate(error: String)
      extends ManagementServiceError(error)
  case MismatchingSchemas(error: String) extends ManagementServiceError(error)
  case InvalidConstraints(errors: List[String])
      extends ManagementServiceError(
        s"Validation errors: ${errors.map(_.replace(":", "")).mkString(",")}"
      )
  case InstanceValidationError(errors: List[String])
      extends ManagementServiceError(
        s"Validation errors: ${errors.map(_.replace(":", "")).mkString(",")}"
      )
  case MapperInstanceValidationError(error: String)
      extends ManagementServiceError(error)
  case InvalidMappingError(error: String) extends ManagementServiceError(error)
  case MappingCycleDetectedError(error: String)
      extends ManagementServiceError(error)
  case MappingNotFoundError(error: String) extends ManagementServiceError(error)
  case UpdatedTypeIsMappingTargetError(entityTypeName: String)
      extends ManagementServiceError(
        s"The entity $entityTypeName is the target of a mapping"
      )
end ManagementServiceError
