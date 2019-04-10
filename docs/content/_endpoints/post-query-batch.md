---
method: POST
endpoint: /query/batch
help: Perform a batch query
description: Run multiple metrics query in a batch.
fields:
- name: '*'
  required: true
  type_name: Query
  purpose: Queries to run.
response_fields:
- name: '*'
  type_name: QueryResponse
  purpose: Responses to each query run.
---
This accepts a JSON document where all keys are expected to map up to a Query.
