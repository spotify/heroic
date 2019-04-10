---
method: POST
endpoint: /metadata/tags
help: Search for tags metadata
description: Query for available tag combinations.
request: metadata-query-body.json
fields:
- name: filter
  required: true
  type_name: Filter
  purpose: A filter to use when quering for tags.
---
