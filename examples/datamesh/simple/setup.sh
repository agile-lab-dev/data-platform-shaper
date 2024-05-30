#!/bin/bash

curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/bulkCreation/yaml' \
  -H 'accept: application/octet-stream' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@DataPlatformShape.yaml'

echo ""

result1=$(curl -s -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity/yaml' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@DataProduct.yaml')

echo "$result1"

result2=$(curl -s -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity/yaml' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@FileBasedOutputPort.yaml')

echo "$result2"

curl -X 'POST' \
  "http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity/link/$result1/hasPart/$result2" \
  -H 'accept: application/text' \
  -d ''

echo ""


result3=$(curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity/yaml' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '@TableBasedOutputPort.yaml')

echo "$result3"

curl -X 'POST' \
  "http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity/link/$result3/dependsOn/$result2" \
  -H 'accept: application/text' \
  -d ''

echo ""

curl -X 'POST' \
  "http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/entity/link/$result1/hasPart/$result3" \
  -H 'accept: application/text' \
  -d ''

echo ""


curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/mapping/mappedInstances' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/text' \
  -d "$result2"

echo ""

curl -X 'POST' \
  'http://127.0.0.1:8093/dataplatform.shaper.uservice/0.0/ontology/mapping/mappedInstances' \
  -H 'accept: application/text' \
  -H 'Content-Type: application/text' \
  -d "$result3"

echo ""
