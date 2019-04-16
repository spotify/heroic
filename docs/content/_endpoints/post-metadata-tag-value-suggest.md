---
method: POST
endpoint: /metadata/tag-value-suggest
help: Search for tag value suggestions
description: Search for tag values.
fields:
- name: key
  type_json: '&lt;string&gt;'
  purpose: Key for where to search for tag values.
- name: filter
  type_name: Filter
  purpose: Filter to apply
- name: range
  type_name: QueryDateRange
  purpose: The time range for which to get suggestions.
- name: limit
  type_json: '&lt;number&gt;'
  purpose: The time range for which to get suggestions.
response_fields:
- name: 'errors'
  type_name: 'RequestError'
  type_array: true
  purpose: 'Errors that occured during the request.'
- name: values
  required: true
  type_json: '[&lt;string&gt;, ...]'
  purpose: Values of the given tag.
- name: limited
  required: true
  type_json: '&lt;boolean&gt;'
  purpose: If the result is limited or not.
---
