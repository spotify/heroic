---
method: POST
endpoint: /metadata/keys
help: Search for time series keys
description: Use to query for keys.
request: metadata-query-body.json
fields:
- name: filter
  required: true
  type_name: Filter
  purpose: A filter to use when quering for tags.
---
