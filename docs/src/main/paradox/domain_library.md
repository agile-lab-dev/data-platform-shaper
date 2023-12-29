# Domain Library
The system is built on a knowledge graph supporting [rdf4j](https://rdf4j.org). We currently support [Ontotext Graphdb](https://graphdb.ontotext.com) and [OpenLink Software Virtuoso](https://virtuoso.openlinksw.com).
The predefined L0 and L1 ontologies initialize the knowledge graph, and then all the mechanisms for creating and managing traits, trait relationships, entity types (user-defined types), and entities (user-defined type instances) are implemented in a [Scala 3](https://scala-lang.org)-based library, domain library.
The domain library uses [cats-effect](https://typelevel.org/cats-effect/) using a [tagless-final](https://okmij.org/ftp/tagless-final/index.html) approach.

To use the library, a basic knowledge of [cats](https://typelevel.org/cats/) and [cats-effect](https://typelevel.org/cats-effect/) is needed.

## Getting started
### Setup a main
Let's suppose we want to use the library inside our main, using [cats-effect](https://typelevel.org/cats-effect/); we need to create an object class extending IOapp, with a special method run:

```scala
import cats.effect.{ExitCode, IO, IOApp}

object Main extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    IO.pure(()).as(ExitCode.Success)
  end run
end Main
```
## Create a database session
Now, the first thing to do is connect to the knowledge graph database. For example, let's do it with the [OpenLink Software Virtuoso](https://virtuoso.openlinksw.com) by running the free version of it:

```
docker run \
    --env DBA_PASSWORD=mysecret \
    --publish 1111:1111 \
    --publish 8890:8890 \
    openlink/virtuoso-opensource-7:latest
```

Then, we can create a database session:

```scala
package it.agilelab.dataplatformshaper.domain

import cats.effect.{ExitCode, IO, IOApp}
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.Session

object Main extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    val session = Session[IO](
      "virtuoso",
      "localhost",
      1111,
      "dba",
      "mysecret",
      "repo1",
      false
    )
    
    session.use(
      session =>
        IO.pure(())
    ).as(ExitCode.Success)
    
  end run
end Main
```

Notice the ```session.use```; it takes a function with a ```session``` as an argument; this function can contain all the business logic we want to implement using the domain library.
## Create the repository
With a session then, we can create a repository:

```scala
package it.agilelab.dataplatformshaper.domain

import cats.effect.{ExitCode, IO, IOApp}
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{Rdf4jKnowledgeGraph, Session}

object Main extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    val session = Session[IO](
      "virtuoso",
      "localhost",
      1111,
      "dba",
      "mysecret",
      "repo1",
      false
    )

    session.use(
      session =>
        val repository: Rdf4jKnowledgeGraph[IO] = Rdf4jKnowledgeGraph[IO](session)
        IO.pure(())
    ).as(ExitCode.Success)

  end run
end Main
```

### Create the services
With the```repository```it's possible to create all the *services* needed for managing traits, entity types, and entities. The example below shows a for-comprehension using a monad transformer [EitherT](https://typelevel.org/cats/datatypes/eithert.html). Using [EitherT](https://typelevel.org/cats/datatypes/eithert.html) in this way allows an excellent composition of all the initialization steps with all the methods implemented by the services.

All the service methods return something in the form of```F[Either[ManagementServiceError, T]```, using EitherT it's then possible to compose the service invocation in a for-comprehension, as shown below.


```scala
package it.agilelab.dataplatformshaper.domain

import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Ref}
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{Rdf4jKnowledgeGraph, Session}
import it.agilelab.dataplatformshaper.domain.model.l0.EntityType
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import it.agilelab.dataplatformshaper.domain.service.interpreter.{InstanceManagementServiceInterpreter, TraitManagementServiceInterpreter, TypeManagementServiceInterpreter}

object Main extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    val session = Session[IO](
      "virtuoso",
      "localhost",
      1111,
      "dba",
      "mysecret",
      "repo1",
      false
    )

    session.use(
      session =>
        val repository: Rdf4jKnowledgeGraph[IO] = Rdf4jKnowledgeGraph[IO](session)

        val typeCacheRef: IO[Ref[IO, Map[String, EntityType]]] = Ref[IO].of(Map.empty[String, EntityType])

        for {
          _ <-     EitherT(repository.loadBaseOntologies().map(Right[ManagementServiceError, Unit]))
          cache <- EitherT(typeCacheRef.map(Right[ManagementServiceError, Ref[IO, Map[String, EntityType]]]))
          trms = new TraitManagementServiceInterpreter(repository)
          tms  = {
            given Ref[IO, Map[String, EntityType]] = cache
            new TypeManagementServiceInterpreter[IO](trms)
          }
          ims = new InstanceManagementServiceInterpreter(tms)
        } yield ()

        IO.pure(())
    ).as(ExitCode.Success)

  end run
end Main
```
In the example above, we created three services```trms```,```tms```, and```ims```, respectively as instances of```TraitManagementServiceInterpreter```, ```TypeManagementServiceInterpreter```, and```InstanceManagementServiceInterpreter```.
The first provides a *[CRUD](https://en.wikipedia.org/wiki/Create,_read,_update_and_delete)* like service for managing traits, the second a *CRUD* like service for managing entity types, and the latter a *CRUD* like service for managing entities.
###Using the services
Now we can use the services; in the example below, we show how to create an entity type, instantiate an entity from that entity type, delete the created instance, and, finally, delete the entity type:

```scala
package it.agilelab.dataplatformshaper.domain

import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Ref}
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{Rdf4jKnowledgeGraph, Session}
import it.agilelab.dataplatformshaper.domain.model.l0.EntityType
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import it.agilelab.dataplatformshaper.domain.service.interpreter.{InstanceManagementServiceInterpreter, TraitManagementServiceInterpreter, TypeManagementServiceInterpreter}

object Main extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    val session = Session[IO](
      "virtuoso",
      "localhost",
      1111,
      "dba",
      "mysecret",
      "repo1",
      false
    )

    session.use(
      session =>
        val repository: Rdf4jKnowledgeGraph[IO] = Rdf4jKnowledgeGraph[IO](session)
        
        val typeCacheRef: IO[Ref[IO, Map[String, EntityType]]] = Ref[IO].of(Map.empty[String, EntityType])
      
        (for {
          _ <-     EitherT(repository.loadBaseOntologies().map(Right[ManagementServiceError, Unit]))
          cache <- EitherT(typeCacheRef.map(Right[ManagementServiceError, Ref[IO, Map[String, EntityType]]]))
          trms = new TraitManagementServiceInterpreter(repository)
          tms  = {
            given Ref[IO, Map[String, EntityType]] = cache
            new TypeManagementServiceInterpreter[IO](trms)
          }
          ims = new InstanceManagementServiceInterpreter(tms)
          cr <-    EitherT(tms.create(EntityType(
            "DataProductType",
            Set(),
            StructType(List("name" -> StringType(), "domain" -> StringType())): Schema
          )))
          ci <-    EitherT(ims.create("DataProductType", ("name" -> "dp1", "domain" -> "HR")))
          id <-    EitherT(ims.delete(ci))
          td <-    EitherT(tms.delete("DataProductType"))
        } yield List(cr, ci, id, td)).value
    ).map(
      _.map(
        ret =>
          println(ret.mkString("\n"))
      )).as(ExitCode.Success)
  end run
end Main
```
##How to define a schema
To define a schema, you have to import: ```it.agilelab.dataplatformshaper.domain.model.schema```, then you have the following attribute types available:

```
StringType         //A Scala String
IntType            //A Scala Int
DateType           //Mapped to java.time.LocalDate 
TimestampDataType  //Mapped to java.time.ZonedDateTime
DoubleType         //A Scala Double
FloatType          //A Scala Float
LongType           //A Scala Long
BooleanType        //A Scala Boolean
StructType         //A structure meant to hold attributes in a nested structure.
```

Each attribute type has an attribute ```mode``` with type SqlTypeMode. Mode is an enum with values: Nullable, Required, Repeated.

* Nullable means an attribute can be optional; the value will be mapped into a specific Option type.
* Required means that an attribute is mandatory.
* Repeated means that the attribute contains a list of values of the specified type.

Let's start to define a schema. Before, from the project root run: ```sbt 'domain/console'```.
Now, you have a Scala shell with all the domain library classes available.

### A simple flat schema with a list of attributes

```
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.model.schema.{*, given}
import io.circe.yaml.*
import io.circe.yaml.syntax.*

val schema: Schema = StructType(
      List(
        "organization" -> StringType(),
        "sub-organization" -> StringType(),
        "domain" -> StringType(),
        "sub-domain" -> StringType()
        )
      )  
```
A schema is a StructType with Required SqlTypeMode. In the example above, all the attributes are *Required*. A conformed tuple should have the following structure:

```scala
val tuple = (
     "organization" -> "Italy",
     "sub-organization" -> "HR",
     "domain" -> "Registration",
     "sub-domain" -> "People"
  )
```
It's possible to parse a tuple to check if it's conform to the schema:

```scala
parseTuple(tuple, schema: Schema)
```

We can also convert a tuple into a JSON ([Circe](https://circe.github.io/circe/)) representation:

```scala
val json = tupleToJson(tuple, schema).toOption.get
```

And, convert back a JSON into a tuple:

```scala
val tuple = jsonToTuple(json, schema)
```

###A schema with deep nesting

```scala
val schema: Schema = StructType(
      List(
        "organization" -> StringType(),
        "sub-organization" -> StringType(),
        "domain" -> StringType(),
        "sub-domain" -> StringType(),
        "nested" -> StructType(List(
             "nestedField1" -> StringType(),
             "nestedField2" -> StringType(),
             "furtherNested" -> StructType(List(
                 "furtherNested1" -> StringType(),
                 "furtherNested2" -> StringType()
               )
             )
           )  
        )
       )
     )  
```
A conformed tuple:

```scala
val tuple = (
     "organization" -> "Italy",
     "sub-organization" -> "HR",
     "domain" -> "Registration",
     "sub-domain" -> "People",
     "nested" -> (
       "nestedField1" -> "nest1",
       "nestedField2" -> "nest2",
       "furtherNested" -> (
          "furtherNested1" -> "fnest3",
          "furtherNested2" -> "fnest4"
       )
     )
  )
  
val json = tupleToJson(tuple, schema).toOption.get
val tuple = jsonToTuple(json, schema)
  
```
###Nullable and repeated attributes

```scala
val schema: Schema = StructType(
      List(
        "organization" -> StringType(),
        "sub-organization" -> StringType(),
        "domain" -> StringType(),
        "sub-domain" -> StringType(),
        "nested" -> StructType(List(
             "nestedField1" -> StringType(),
             "nestedField2" -> StringType(),
             "furtherNested" -> StructType(List(
                 "furtherNested1" -> StringType(),
                 "furtherNested2" -> StringType()
               )
             )
           )  
        ),
       "repeated" -> StringType(Repeated),
       "nullable" -> StringType(Nullable),
       "anotherNullable" -> StringType(Nullable)
       )
     )
```
A conformed tuple:

```scala
val tuple = (
     "organization" -> "Italy",
     "sub-organization" -> "HR",
     "domain" -> "Registration",
     "sub-domain" -> "People",
     "nested" -> (
       "nestedField1" -> "nest1",
       "nestedField2" -> "nest2",
       "furtherNested" -> (
          "furtherNested1" -> "fnest3",
          "furtherNested2" -> "fnest4"
       )
     ),
     "repeated" -> List("label2", "label2", "label3"),
     "nullable" -> None,
     "anotherNullable" -> Some("someString")
  )
  
val json = tupleToJson(tuple, schema).toOption.get
val tuple = jsonToTuple(json, schema)
```
Let's see the YAML generated starting from a tuple:

```scala
println(json.asYaml.spaces2)

```
###Repeated struct

```scala
val schema: Schema = StructType(
      List(
        "organization" -> StringType(),
        "sub-organization" -> StringType(),
        "domain" -> StringType(),
        "sub-domain" -> StringType(),
        "repeated" -> StructType(
                        List(
                          "nestedField1"  -> StringType(),
                          "nestedField2"  -> StringType(),
                        ),   
                        Repeated
                      )
      )
   )    
```
A conformed tuple:

```scala
val tuple = (
     "organization" -> "Italy",
     "sub-organization" -> "HR",
     "domain" -> "Registration",
     "sub-domain" -> "People",
     "repeated" -> List(
       (
         "nestedField1" -> "nest1",
         "nestedField2" -> "nest2",
       ),
       (
         "nestedField1" -> "nest3",
         "nestedField2" -> "nest4",
       ),
       (
         "nestedField1" -> "nest5",
         "nestedField2" -> "nest6",
       ),
       (
         "nestedField1" -> "nest7",
         "nestedField2" -> "nest8",
       ),         
     )
  )
  
val json = tupleToJson(tuple, schema).toOption.get
val tuple = jsonToTuple(json, schema)
println(json.asYaml.spaces2)
```
##How to use the services
###Trait management service
Below is the exposed interface:

```scala
trait TraitManagementService[F[_]]:

  val repository: KnowledgeGraph[F]

  def create(
      traitName: String,
      inheritsFrom: Option[String]
  ): F[Either[ManagementServiceError, Unit]]

  def exist(
      traitName: String
  ): F[Either[ManagementServiceError, Boolean]]

  def exist(
      traitNames: Set[String]
  ): F[Either[ManagementServiceError, Set[(String, Boolean)]]]

  def link(
      trait1Name: String,
      linkType: Relationship,
      traitName2: String
  ): F[Either[ManagementServiceError, Unit]]

  def unlink(
      trait1Name: String,
      linkType: Relationship,
      trait2Name: String
  ): F[Either[ManagementServiceError, Unit]]

  def linked(
      traitName: String,
      linkType: Relationship
  ): F[Either[ManagementServiceError, List[String]]]

end TraitManagementService
```

####Create a trait
```scala
trms.create("aTraitName", Some("fatherTrait"))
```
It creates a trait names *aTraitName*, inheriting from *fatherTrait*:
####Link two traits
```scala
session.use { session =>
      val repository = Rdf4jKnowledgeGraph[IO](session)
      val trms = TraitManagementServiceInterpreter[IO](repository)
      (for {
        _ <- EitherT(trms.create("DataProductComponent", None))
        _ <- EitherT(trms.create("OutputPort", Some("DataProductComponent")))
        _ <- EitherT(trms.create("DataProduct", None))
        res <- EitherT(
          trms
            .link("DataProduct", Relationship.hasPart, "DataProductComponent")
        )
      } yield res).value
    }    
```
In this example, we create a trait *DataProductComponent*and a trait inheriting it *OutputPort*. Then, after creating another trait *DataProduct*, we link *DataProduct* with *DataProductComponent* with the predefined relationship *hasPart*.
#### Get the linked traits
```scala
session.use { session =>
      val repository = Rdf4jKnowledgeGraph[IO](session)
      val trms = TraitManagementServiceInterpreter[IO](repository)
      (for {
        res <- EitherT(trms.linked("DataProduct", Relationship.hasPart))
      } yield res).value
    }
```
How to get the list of traits linked to the trait *DataProduct* with the relationship *hasPart*.

#### Unlink two traits
```scala
session.use { session =>
      val repository = Rdf4jKnowledgeGraph[IO](session)
      val trms = TraitManagementServiceInterpreter[IO](repository)
      (for {
        res <- EitherT(trms.unlink("DataProduct", Relationship.hasPart, "DataProductComponent"))
      } yield res).value
    }
```
###Type management service
Below is the exposed interface:

```scala
trait TypeManagementService[F[_]]:

  val repository: KnowledgeGraph[F]

  def create(entityType: EntityType): F[Either[ManagementServiceError, Unit]]

  def create(
      entityType: EntityType,
      inheritsFrom: String
  ): F[Either[ManagementServiceError, Unit]]

  def read(
      instanceTypeName: String
  ): F[Either[ManagementServiceError, EntityType]]

  def delete(instanceTypeName: String): F[Either[ManagementServiceError, Unit]]

  def exist(
      instanceTypeName: String
  ): F[Either[ManagementServiceError, Boolean]]

end TypeManagementService
```

It's a *CRUD* service that manages instances of *EntityType*:

```scala
final case class EntityType(
    name: String,
    traits: Set[String],
    baseSchema: Schema,
    father: Option[EntityType]
):
  def schema: Schema =
    var sc = baseSchema
    father.foreach(et =>
      val scSchemaFieldNames = sc.records.map(_(0)).toSet
      sc = sc.copy(records =
        et.schema.records.filter(r => !scSchemaFieldNames(r(0))) ++ sc.records
      )
    )
    sc
  end schema

end EntityType
```
The *baseSchema* is the schema used for the type definition, where the method *schema* returns the schema merging the inherited attributes.

The companion object EntityType provides different apply methods, allowing the creation of EntityType instances with proper shortcuts:

```scala
object EntityType:
  def apply(name: String, traits: Set[String], initialSchema: Schema): EntityType 

  def apply(name: String, traits: Set[String], initialSchema: Schema, fatherEntityType: EntityType): EntityType

  def apply(name: String, initialSchema: Schema, fatherEntityType: EntityType): EntityType
  
  def apply(name: String, initialSchema: Schema): EntityType
end EntityType
```

####Create and retrieve an entity type
The code below shows how to create an entity type:

```scala
val entityTypeSchema: Schema = StructType(
           List(
             "organization" -> StringType(),
             "sub-organization" -> StringType(),
             "domain" -> StringType(),
             "sub-domain" -> StringType(),
             "repeated" -> StructType(
                             List(
                               "nestedField1"  -> StringType(),
                               "nestedField2"  -> StringType(),
                             ),   
                             Repeated
                           )
           )
        )    

val entityType = EntityType("MyUserDefinedType", Set["ATrait", "AnotherTrait"], schema, None)

session.use(session =>
  val repository = Rdf4jKnowledgeGraph[IO](session)
  val trms       = new TraitManagementServiceInterpreter[IO](repository)
  val tms.      = new TypeManagementServiceInterpreter[IO](trservice)
  (for {
    etype <- EitherT[IO, ManagementServiceError, Unit](
      tms.create(entityType)
    )
  } yield etype).value
)
  
```
When the type to inherit from it is unavailable, there is a particular form of the method *create*:

```scala
tms.create(entityType, "fatherType")

```

Look how to retrieve an entity type:

```scala
tms.read("MyUserDefinedType")
```
###Instance management service
Below is the exposed interface:

```scala
trait InstanceManagementService[F[_]]:

  val repository: KnowledgeGraph[F]

  def create(
      instanceTypeName: String,
      values: Tuple
  ): F[Either[ManagementServiceError, String]]

  def read(instanceId: String): F[Either[ManagementServiceError, Entity]]

  def update(
      instanceId: String,
      values: Tuple
  ): F[Either[ManagementServiceError, String]]

  def delete(
      instanceId: String
  ): F[Either[ManagementServiceError, Unit]]

  def exist(instanceId: String): F[Either[ManagementServiceError, Boolean]]

  def list(
      instanceTypeName: String,
      predicate: Option[SearchPredicate],
      returnEntities: Boolean
  ): F[Either[ManagementServiceError, List[String | Entity]]]

  def list(
      instanceTypeName: String,
      predicate: String,
      returnEntities: Boolean
  ): F[Either[ManagementServiceError, List[String | Entity]]]

  def link(
      instanceId1: String,
      linkType: Relationship,
      instanceId2: String
  ): F[Either[ManagementServiceError, Unit]]

  def unlink(
      instanceId1: String,
      linkType: Relationship,
      instanceId2: String
  ): F[Either[ManagementServiceError, Unit]]

  def linked(
      instanceId: String,
      linkType: Relationship
  ): F[Either[ManagementServiceError, List[String]]]

end InstanceManagementService
```

The meaning of the methods should be pretty straightforward. The *create* method takes an entity type name and a tuple that should conform to the schema associated with the entity type, the actual schema, i.e., the schema containing all the inherited attributes.

The list methods provide ways to retrieve a list of entities as a list of IDs or the actual entity instances.

The link, unlink, and linked methods have similar semantics to the trait counterparts.

