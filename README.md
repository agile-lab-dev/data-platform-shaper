![build-badge][]

# Data Platform Shaper: An RDF-based specialized catalog system for defining and managing data platform assets.

## Introduction

The design and management of modern big data platforms are highly complex. 
It requires carefully integrating multiple storage and computational platforms and implementing approaches to protect and audit data access. 

Therefore, onboarding new data and implementing new data transformation processes is typically time-consuming and expensive. Enterprises often construct their data platforms without distinguishing between logical and technical concerns. 
Consequently, these platforms lack sufficient abstraction and are closely tied to particular technologies, making the adaptation to technological evolution very costly. 

A data platform designer should approach the design of a complex data platform using a logical model where he defines his concept of data collection with its metadata attributes, including the relationship with other items. 
As a more complex example, a logical model of a data mesh is based on assets like the data product and the relationship with its constituent parts like output and input ports.

Once the logical model has been defined, it's possible to determine the rules for mapping the different kinds of assets into the available technology. From a data platform analytical model to the corresponding physical model, this approach is paramount to making any data platform technologically agnostic and resilient to the evolution of technology.

This project aims to implement an RDF-based catalog supporting a novel approach to designing data platform models based on a formal ontology that structures various domain components across distinct levels of abstraction. 

This catalog should be the base for defining data platform assets and ontologies capable of describing data collection, data mesh, and data products, i.e., the typical items that compose a modern data platform.

## How to build and run the project

The project is currently based on the [rdf4j](https://rdf4j.org) library and the [GraphDB](https://graphdb.ontotext.com) knowledge graph from Ontotext.

It's a Scala 3 project based on the [sbt building](https://www.scala-sbt.org) tool that needs to be previously installed; for running the tests, you need to have a docker daemon up and running, please check that the docker daemon is accessible from a non-root user. The tests are performed using a GraphDB instance running in a container. A docker-compose file is provided for running GraphDB and the microservice together.

You also need to have [Cue](https://cuelang.org/docs/introduction/installation/) installed.

### Build and Test

This is a Scala-based project; we used [Scala 3](https://www.scala-lang.org) in combination with the [Typelevel libraries](https://typelevel.org), in particular, [Cats Effect](https://typelevel.org/cats-effect/) for managing effect using the tag-less final pattern. 
```
git clone https://github.com/agile-lab-dev/data-platform-shaper.git
cd data-platform-shaper
sbt compile test
```

### Build the documentation

```
sbt paradox previewSite
```
Then open this [URL](http://localhost:4000/paradox/site/main/).
### Run everything
You can run GraphDB and the microservice together using a docker-compose file, so first build the image locally:

```
sbt docker:publishLocal
```

Then run everything with:

```
docker compose up
```

After a while, you can connect to the Swagger UI [here](http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/swagger-ui/index.html).

Then, you can try to create a user-defined type:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity-type' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "DataCollectionType",
  "traits": [],
  "schema": [
    {
      "name": "name",
      "typeName": "String",
      "mode": "Required"
    },
    {
      "name": "organization",
      "typeName": "String",
      "mode": "Required"
    },
    {
      "name": "domain",
      "typeName": "String",
      "mode": "Required"
    }
  ]
}'
```

You could create a user-defined type posting a YAML file:

```
name: DataCollectionType
traits:
schema:
- name: name
  typeName: String
  mode: Required
  attributeTypes: null
- name: organization
  typeName: String
  mode: Required
  attributeTypes: null
- name: domain
  typeName: String
  mode: Required
  attributeTypes: null
fatherName: null
```

Just put the content in a file entity-type.yaml, and then:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity-type/yaml' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@entity-type.yaml'
```

After creating the user-defined type, it's now possible to create instances of it, either using a REST API or just posting a YAML document:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/json' \
  -d '{
  "entityId": "",
  "entityTypeName": "DataCollectionType",
  "values": {
    "name": "Person",
    "organization": "HR",
    "domain": "Registrations"
  }
}'
```

YAML file (put it into a file entity.yaml):

```
entityId: ""
entityTypeName: DataCollectionType
values:
  domain: Registrations
  organization: HR
  name: Person
```

And submit it:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity/yaml' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@entity.yaml'
```

Creating an instance returns its ID, so you can use that ID for retrieving it:

```
curl -X 'GET' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity/9c2a7c0a-575d-4572-9d33-610d7b4977af' \
  -H 'accept: application/json'
```

### A more complex example

In this example, we will create two traits and link them to show how the trait relationship is automatically inherited by the instances of types linked to those traits.

Let's create the first trait, representing a data collection: 

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/trait' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "DataCollection"
}'
```

Let's create the second trait representing a table schema:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/trait' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "TableSchema"
}'
```

Now, we link the two traits, representing the fact that a data collection could be associated with a table schema:

```
curl -X 'PUT' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/trait/link/DataCollection/hasPart/TableSchema' \
  -H 'accept: application/json'
```

Now, let's create a type DataCollectionType using the trait DataCollection:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity-type' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "DataCollectionType",
  "traits": [
    "DataCollection"
  ],
  "schema": [
    {
      "name": "name",
      "typeName": "String",
      "mode": "Required"
    },
    {
      "name": "organization",
      "typeName": "String",
      "mode": "Required"
    },
    {
      "name": "domain",
      "typeName": "String",
      "mode": "Required"
    }
  ]
}'
```

Alternatively, you could use the following YAML file:

```
name: DataCollectionType
traits:
- DataCollection
schema:
- name: name
  typeName: String
  mode: Required
- name: organization
  typeName: String
  mode: Required
- name: domain
  typeName: String
  mode: Required

```
And the following call:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity-type/yaml' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@data-collection-type.yaml'
```

Next, let's create a type TableSchemaType using the trait TableSchema:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity-type' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "TableSchemaType",
  "traits": [
    "TableSchema"
  ],
  "schema": [
    {
      "name": "tableName",
      "typeName": "String",
      "mode": "Required"
    },
    {
      "name": "columns",
      "typeName": "Struct",
      "mode": "Repeated",
      "attributeTypes": [
            {
              "name": "columnName",
              "typeName": "String",
              "mode": "Required"
            },
            {
              "name": "columnType",
              "typeName": "String",
              "mode": "Required"
            }
      ]
    }
  ]
}'
```

Or by using the following YAML file:

```
name: TableSchemaType
traits:
- TableSchema
schema:
- name: tableName
  typeName: String
  mode: Required
- name: columns
  typeName: Struct
  mode: Repeated
  attributeTypes:
    - name: columnName
      typeName: String 
      mode: Required
    - name: columnType
      typeName: String
      mode: Required
```

And the corresponding call:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity-type/yaml' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@table-schema-type.yaml'
```

At this point, we have two types, DataCollectionType and TableSchemaType, associated with the traits DataCollection and TableSchema. So, since we have this relationship in place, we can associate any instance of DataCollectionType with any instance of TableSchemaType with the relationship we used for linking the two traits.

Let's first create an instance of a DataCollectionType:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/json' \
  -d '{
  "entityId": "",
  "entityTypeName": "DataCollectionType",
  "values": {
     "name": "Person",
     "organization": "HR",
     "domain": "Registrations"
   }
}
'
```

This call will return the id of the newly created instance, for example: ```29147f92-db7a-41be-abe1-a28f29418ce1```

Next, let's create an instance of a TableSchemaType:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/json' \
  -d '{
  "entityId": "",
  "entityTypeName": "TableSchemaType",
  "values": {
    "tableName": "Person",
    "columns": [
       {
         "columnName": "firstName",
         "columnType": "String"
       },
       {
         "columnName": "familyName",
         "columnType": "String"
       },
       {
         "columnName": "age",
         "columnType": "Int"
       }
    ]
  }
}'
```

catching the instance id: ```51a2af02-6504-4c12-8cc4-8dd3874af5c4```.

At this point, it is possible to link the two instances with the same relationship we used for linking the two traits associated with the two instance types:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity/link/29147f92-db7a-41be-abe1-a28f29418ce1/hasPart/51a2af02-6504-4c12-8cc4-8dd3874af5c4' \
  -H 'accept: application/text'
```

It's also possible to search instances by attribute values:

```
curl -X 'GET' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity?entityTypeName=DataCollectionType&query=name%20%3D%20%27Person%27' \
  -H 'accept: application/json'
```
Given a search string (name = 'Person'), this API returns a list of instance IDs.
Another API given a search string returns a list of instances:

```
curl -X 'GET' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity/id?entityTypeName=DataCollectionType&query=name%20%3D%20%27Person%27' \
  -H 'accept: application/json'
```

That's all!

**After a run with ```docker compose up``` you should always run ```docker compose rm``` to clean up everything.**

#Attention

It's still a work in progress; more documentation explaining the overall model and the internal APIs will be written.
You can look at the various tests to get a better understanding of its internal working:

* This test suite shows how the system manages tuples, parsing, and unparsing driven by a schema
```
domain/src/test/scala/it/agilelab/dataplatformshaper/domain/DataTypeSpec.scala 
```
* This test suite shows the inheritance mechanism:
```
domain/src/test/scala/it/agilelab/dataplatformshaper/domain/EntityTypeSpec.scala
```
* This test suite shows the user-defined types and their instances creation:
```
domain/src/test/scala/it/agilelab/dataplatformshaper/domain/OntologyL0Spec.scala
```
* This test suite shows everything about traits and their usage:
```
domain/src/test/scala/it/agilelab/dataplatformshaper/domain/OntologyL1Spec.scala
```
* This test suite shows the REST API usage:
```
uservice/src/test/scala/it/agilelab/dataplatformshaper/uservice/api/ApiSpec.scala
```

#Credits

This project is the result of a collaborative effort:

| Name                      | Affiliation                                                                           |
| ------------------------- | --------------------------------------------------------------------------------------|
| Diego Reforgiato Recupero | Department of Math and Computer Science, University of Cagliari (Italy)               |
| Francesco Osborne         | KMi, The Open University (UK) and University of Milano-Bicocca (Italy)                |
| Andrea Giovanni Nuzzolese | Institute of Cognitive Sciences and Technologies National Council of Research (Italy) |
| Simone Pusceddu.          | Department of Math and Computer Science, University of Cagliari (Italy)               |
| David Greco               | Big Data Laboratory, AgileLab S.r.L. (Italy)                                          |
| Nicolò Bidotti            | Big Data Laboratory, AgileLab S.r.L. (Italy)                                          |
| Paolo Platter             | Big Data Laboratory, AgileLab S.r.L. (Italy)                                          |

[build-badge]: https://github.com/agile-lap-dev/data-platform-shaper/actions/workflows/test.yml/badge.svg
