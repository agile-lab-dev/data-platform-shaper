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
With the```repository```it's possible to create all the *services* needed for managing traits, entity types, and entities. The example below shows a for comprehension using a monad transformer [EitherT](https://typelevel.org/cats/datatypes/eithert.html). Using [EitherT](https://typelevel.org/cats/datatypes/eithert.html) in this way allows an excellent composition of all the initialization steps with all the methods implemented by the services.


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
=======


>>>>>>> 00ca1ec (some code restructuring)
