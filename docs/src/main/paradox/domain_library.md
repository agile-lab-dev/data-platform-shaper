# Domain Library
The system is built on a knowledge graph supporting [rdf4j](https://rdf4j.org). We currently support [Ontotext Graphdb](https://graphdb.ontotext.com) and [OpenLink Software Virtuoso](https://virtuoso.openlinksw.com).
The predefined L0 and L1 ontologies initialize the knowledge graph, and then all the mechanisms for creating and managing traits, trait relationships, entity types (user-defined types), and entities (user-defined type instances) are implemented in a [Scala 3](https://scala-lang.org)-based library, domain library.
The domain library uses [cats-effect](https://typelevel.org/cats-effect/) using a [tagless-final](https://okmij.org/ftp/tagless-final/index.html) approach.



