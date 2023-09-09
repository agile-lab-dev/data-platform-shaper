package it.agilelab.witboost.ontology.manager.domain.model

import org.eclipse.rdf4j.model.impl.SimpleNamespace
import org.eclipse.rdf4j.model.util.Values.iri

object NS:

  val ns = new SimpleNamespace("ns", "https://w3id.org/agile-dm/ontology/")

  val L0 = iri(ns, "L0") // Base Ontology
  val L1 = iri(ns, "L1") // Traits
  val L2 = iri(ns, "L2") // User Defined Types
  val L3 = iri(ns, "L3") // Instances

  // L0
  val TYPENAME = iri(ns, "typeName")
  val HASTRAIT = iri(ns, "hasTrait")
  val ENTITYTYPE = iri(ns, "EntityType")
  val INHERITSFROM = iri(ns, "inheritsFrom")
  val ISCLASSIFIEDBY = iri(ns, "isClassifiedBy")
  val STRINGATTRIBUTETYPE = iri(ns, "StringAttributeType")
  val INTATTRIBUTETYPE = iri(ns, "IntAttributeType")
  val HASATTRIBUTETYPE = iri(ns, "hasAttributeType")
  val ENTITY = iri(ns, "Entity")
  val MODE = iri(ns, "mode")
  val REQUIRED = iri(ns, "Required")
  val REPEATED = iri(ns, "Repeated")
  val NULLABLE = iri(ns, "Nullable")
  val STRUCTTYPE = iri(ns, "StructType")
  val STRUCT = iri(ns, "Struct")

  // L1

end NS
