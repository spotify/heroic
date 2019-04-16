---
method: DELETE
endpoint: /metadata/series
help: Delete time series metadata
description: Delete all series metadata matching the given filter.
request: metadata-query-body.json
fields:
- name: filter
  required: true
  type_name: Filter
  purpose: A filter to use when quering for tags.
---
