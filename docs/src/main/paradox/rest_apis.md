# REST APIs

## OpenAPI Interface
At the link below, you can find the complete OpenApi specification:

@extref[**OpenAPI Specification**](site:openapi_interface.html)

## How to use the REST APIs

### Creating traits and types

We provide a more complex example to show the primary usage of the REST APIs.
We try to model a more complex example; let's suppose we want to model the concept of DataProduct, the managed asset in a DataMesh as outlined in this [specification](https://github.com/agile-lab-dev/Data-Product-Specification).

At a very high level, a *DataProduct* contains multiple *components* where each component can be an *OutputPort*, a *Workload*, a *StorageArea*, or an *Observability*.

So, we could start modeling by defining first the various traits representing the assets above:

**DataProduct**

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/trait' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "DataProduct"
}'
```

**DataProductComponent**

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/trait' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "DataProductComponent"
}'
```

**OutputPort**

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/trait' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "OutputPort",
  "inheritsFrom": "DataProductComponent"
}'
```

Notice that an *OutputPort* inherits from a *DataProductComponent*.

**Workload**

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/trait' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "Workload",
  "inheritsFrom": "DataProductComponent"
}'
```

Notice that a *Workload* inherits from a *DataProductComponent*.

**StorageArea**

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/trait' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "StorageArea",
  "inheritsFrom": "DataProductComponent"
}'
```

Notice that a *StorageArea* inherits from a *DataProductComponent*.

**Observabiity**

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/trait' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "Observability",
  "inheritsFrom": "DataProductComponent"
}'
```

Notice that an *Observability* inherits from a *DataProductComponent*.


We can link the trait *DataProduct* with the trait *DataProductComponent* using the *hasPart* relationship.

```
curl -X 'PUT' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/trait/link/DataProduct/hasPart/DataProductComponent' \
  -H 'accept: application/json'
```

Having defined all the needed traits, we can start defining the actual types.

**DataProductType**

Let's start with *DataProductType* by putting into a file ```DataProductType.yml``` the following document:

```
name: DataProductType
traits: [DataProduct]
schema:
- name: id
  typeName: String
  mode: Required
- name: name
  typeName: String
  mode: Required
- name: fullyQualifiedName
  typeName: String
  mode: Nullable
- name: description
  typeName: String
  mode: Required
- name: kind
  typeName: String
  mode: Required
- name: domain
  typeName: String
  mode: Required
- name: version
  typeName: String
  mode: Required
- name: environment
  typeName: String
  mode: Required
- name: dataProductOwner
  typeName: String
  mode: Required
- name: email
  typeName: String
  mode: Nullable
- name: ownerGroup
  typeName: String
  mode: Required
- name: devGroup
  typeName: String
  mode: Required
- name: informationSLA
  typeName: String
  mode: Nullable
- name: status
  typeName: String
  mode: Nullable
- name: maturity
  typeName: String
  mode: Nullable
fatherName: null
```

Now, we can create it by invoking the proper REST API:

```curl -X 'POST' \
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity-type/yaml' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@DataProductType.yml'
```

We can repeat the same for creating all the rest of the needed types:

**OutputPortType**

```
OutputPortType.yml
```

```
name: OutputPortType
traits: [OutputPort]
schema:
- name: id
  typeName: String
  mode: Required
- name: name
  typeName: String
  mode: Required
- name: fullyQualifiedName
  typeName: String
  mode: Nullable
- name: description
  typeName: String
  mode: Required
- name: kind
  typeName: String
  mode: Required
- name: version
  typeName: String
  mode: Required
- name: infrastructureTemplateId
  typeName: String
  mode: Required
- name: useCaseTemplateId
  typeName: String
  mode: Nullable
- name: dependsOn
  typeName: String
  mode: Repeated
- name: platform
  typeName: String
  mode: Nullable
- name: technology
  typeName: String
  mode: Nullable
- name: outputPortType
  typeName: String
  mode: Required
- name: creationDate
  typeName: Date
  mode: Nullable
- name: startDate
  typeName: String
  mode: Nullable
- name: retentionTime
  typeName: String
  mode: Nullable
- name: processDescription
  typeName: String
  mode: Nullable
fatherName: null
```

Creation:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity-type/yaml' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@OutputPortType.yml'
```

**WorkloadType**

```
WorkloadType.yml
```
	
```
name: WorkloadType
traits: [Workload]
schema:
- name: id
  typeName: String
  mode: Required
- name: name
  typeName: String
  mode: Required
- name: fullyQualifiedName
  typeName: String
  mode: Nullable
- name: description
  typeName: String
  mode: Required
- name: kind
  typeName: String
  mode: Required
- name: version
  typeName: String
  mode: Required
- name: infrastructureTemplateId
  typeName: String
  mode: Required
- name: useCaseTemplateId
  typeName: String
  mode: Required
- name: dependsOn
  typeName: String
  mode: Repeated
- name: platform
  typeName: String
  mode: Nullable
- name: technology
  typeName: String
  mode: Nullable
- name: workloadType
  typeName: String
  mode: Nullable
- name: connectionType
  typeName: String
  mode: Nullable
- name: readsFrom
  typeName: String
  mode: Repeated
fatherName: null
```

Creation:


```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity-type/yaml' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@WorkloadType.yml'
```

**StorageAreaType**

```
StorageAreaType.yml
```

```
name: StorageAreaType
traits: [StorageArea]
schema:
- name: id
  typeName: String
  mode: Required
- name: name
  typeName: String
  mode: Required
- name: fullyQualifiedName
  typeName: String
  mode: Nullable
- name: description
  typeName: String
  mode: Required
- name: kind
  typeName: String
  mode: Required
- name: owners
  typeName: String
  mode: Repeated
- name: infrastructureTemplateId
  typeName: String
  mode: Required
- name: useCaseTemplateId
  typeName: String
  mode: Nullable
- name: dependsOn
  typeName: String
  mode: Repeated
- name: platform
  typeName: String
  mode: Nullable
- name: technology
  typeName: String
  mode: Nullable
- name: storageType
  typeName: String
  mode: Nullable
fatherName: null
```

Creation:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity-type/yaml' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@StorageAreaType.yml'
```

**ObservabilityType**

```
ObservabilityType.yml
```

```
name: ObservabilityType
traits: [Observability]
schema:
- name: id
  typeName: String
  mode: Required
- name: name
  typeName: String
  mode: Required
- name: fullyQualifiedName
  typeName: String
  mode: Required
- name: description
  typeName: String
  mode: Required
fatherName: null
```

Creation:

```
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity-type/yaml' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@ObservabilityType.yml'
```

We showed a more complex example where a specification that describes a complex data platform asset can be decomposed into different traits and types linked with a "compose" relationship.

