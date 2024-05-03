package it.agilelab.dataplatformshaper.domain.model

import it.agilelab.dataplatformshaper.domain.common.{
  stringEnumDecoder,
  stringEnumEncoder
}

enum Relationship:
  case hasPart
  case mappedTo
  case dependsOn
  case dependencyOf

  def getNamespace: String =
    this match
      case Relationship.hasPart =>
        "http://www.w3.org/2001/sw/BestPractices/OEP/SimplePartWhole/part.owl#"
      case Relationship.mappedTo =>
        "https://w3id.org/agile-dm/ontology/"
      case Relationship.dependsOn =>
        "https://w3id.org/agile-dm/ontology/"
      case Relationship.dependencyOf =>
        "https://w3id.org/agile-dm/ontology/"
    end match
  end getNamespace

  def getInverse: Option[Relationship] =
    this match
      case Relationship.hasPart =>
        None
      case Relationship.mappedTo =>
        None
      case Relationship.dependsOn =>
        Some(Relationship.dependencyOf)
      case Relationship.dependencyOf =>
        Some(Relationship.dependsOn)
    end match
end Relationship

given Conversion[String, Relationship] with
  def apply(str: String): Relationship =
    stringEnumDecoder[Relationship].apply(str)
end given

given Conversion[Relationship, String] with
  def apply(attr: Relationship): String =
    stringEnumEncoder[Relationship].apply(attr)
end given

export Relationship.hasPart as hasPart
export Relationship.mappedTo as mappedTo
export Relationship.dependsOn as dependsOn
export Relationship.dependencyOf as dependencyOf
