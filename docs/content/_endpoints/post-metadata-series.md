---
method: POST
endpoint: /metadata/series
help: Search for time series metadata
description: Get all series metadata matching the given filter.
request: metadata-query-body.json
fields:
- name: filter
  required: true
  type_name: Filter
  purpose: A filter to use when quering for tags.
---
