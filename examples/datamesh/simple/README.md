# Simple Data Mesh Example

## Structure of the example
The diagram of a simple data mesh is shown below:
<div align="center">
  <img src="https://github.com/agile-lab-dev/data-platform-shaper/assets/92328763/4a9c258a-a2d7-4c57-9f37-ecd2c3e32fb1" alt="MappingExample">
</div>

### Conceptual Layer
* **DataProduct**: represents a high-level data product. It has a relationship indicating that it has as a part (*hasPart*) an **OutputPort**;
* **OutputPort**: represents the output interface of the data product, it is *partOf* the **DataProduct**;
* **FileBasedOutputPort**: it's a type of output port dealing with file-based data, it *inheritsFrom* the **OutputPort**;
* **TableBasedOutputPort**: it's a type of output port dealing with table-based data, it *dependsOn* the **FileBasedOutputPort**.

The elements previously described are all Traits.

### Logical Layer
* **DataProductType**: represents different types of data products, it has as a trait (*hasTrait*) the **DataProduct**;
* **FileBasedOutputPortType**: a generic type of file-based output port, it is *partOf* a **DataProductType**;
* **TableBasedOutputPortType**: a generic type of table-based output port, it is *partOf* a **DataProductType**. This output port also *dependsOn* the other output port: **FileBasedOutputPortType**.

The elements previously described are all EntityTypes.

### Physical Layer
* **S3FolderType**: represents a type of storage used in file-based output ports. It maps to the **FileBasedOutputPortType** and has an attribute path;
* **AthenaTableType**: Represents a type of storage used in table-based output ports. It maps to the **TableBasedOutputPortType** and has attributes like dataPath, database, and table.

These elements are also all EntityTypes.

### EntityType Attributes
Both EntityTypes **S3Folder** and **AthenaTable** have attributes that are mapped from attributes which are located in other EntityTypes.<br>
Specifically, the path attribute of the **S3Folder** is created by writing "domain.name" where domain and name are attributes of **DataProductType**.
This will be achieved by using the mapping as shown in [Building the Data Mesh](#building-the-data-mesh).

## Building the Data Mesh
### Creating the Traits and Relationships
In this folder are the YAML files for creating all the entities needed for the example. We begin by creating the needed Traits and Relationships, we do this by using the following curl:

```bash
curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/trait/bulk/yaml' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@traitsAndRelationships.yaml'
```  
Basically the whole [conceptual layer](#conceptual-layer) was created through one curl, which used the contents of the file “traitsAndRelationships.yaml”.
  
### Creating everything in one go
Should you want to create the entire example, the folder contains a bash script called setup.sh that performa all the curls shown above.