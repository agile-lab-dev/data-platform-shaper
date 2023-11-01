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
The project is currently based on the [rdf4j](https://rdf4j.org) library and the [GraphDB](https://graphdb.ontotext.com) knowledge graph from Ontotext. There is a plan to support the RDF4J Native Store.

It's a Scala 3 project based on the [sbt building](https://www.scala-sbt.org) tool that needs to be previously installed; for running the tests, you need to have a docker daemon up and running. The tests are performed using a GraphDB instance running in a container. A docker-compose file is provided for running GraphDB and the microservice together.

### Build and Test
```
git clone https://github.com/agile-lab-dev/data-platform-shaper.git
cd data-platform-shaper
sbt compile test
```
### Run everything
You can run GraphDB and the microservice together using a docker-compose file, so first build the image locally:

```
sbt docker:publishLocal
```

Then run everything with:

```
HOST_IP=<IP ADDRESS OF YOUR MACHINE> docker compose up
```






