---
method: POST
endpoint: /query/metrics
help: Query for metrics
description: Query and aggregate metrics.
type_name: Query
fields:
- name: range
  required: true
  type_name: QueryDateRange
  purpose: 'The range in time for which to query'
- name: filter
  type_name: Filter
- name: aggregation
  type_name: Aggregation
  purpose: Aggregation to use when down-sampling.
- name: groupBy
- name: features
  type_json: '[&lt;string&gt;, ...]'
  purpose: |
    Enable or disable a feature on a per-query basis.
    See <a href="docs/config/features">Features Configuration</a> for more details.
response_fields:
- name: 'range'
  type_json: '{start: &lt;number&gt;, end: &lt;number&gt;}'
  purpose: 'The range in time for which to query'
- name: 'errors'
  type_name: 'RequestError'
  purpose: 'Potential errors returned either from different shards or for specific time series. The presence of an error does not cause the entire query to fail, instead it is up to the client to use this information to decide if the response is reliable enough.'
- name: 'result'
  type_name: 'ShardedResultGroup'
  type_array: true
  purpose: 'An array of result groups.'
- name: 'statistics'
  type_name: 'Statistics'
  purpose: 'Statistics about the current query. This field should be inspected for errors which will have caused the result to be inconsistent.'
---
