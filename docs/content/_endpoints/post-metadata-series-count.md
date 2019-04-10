---
method: POST
endpoint: /metadata/series-count
help: Count time series metadata
description: Count the number of time series matching a given filter.
fields:
- name: filter
  type_name: Filter
  purpose: A filter to use when counting series.
- name: range
  type_name: QueryDateRange
  purpose: Range for which to count series.
---
