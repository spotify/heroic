---
method: POST
endpoint: /metadata/tag-suggest
help: Search for tag suggestions
description: Search for tag suggestions. This endpoint is intended for use with auto-completion.
fields:
- name: key
  type_json: '&lt;string&gt;'
  purpose: Tag key to help limit down suggestions, this is a free text value that is analyzed by the search engine.
- name: value
  type_json: '&lt;string&gt;'
  purpose: Tag value  to help limit down suggestions, this is a free text value that is analyzed by the search engine.
- name: filter
  type_name: Filter
  purpose: Filter to apply
- name: range
  type_name: QueryDateRange
  purpose: The time range for which to get suggestions.
- name: limit
  type_json: '&lt;number&gt;'
  purpose: The time range for which to get suggestions.
- name: match
  type_name: MatchOptions
  purpose: Temporary match options to apply, only used for experiments.
response_fields:
- name: 'errors'
  type_name: 'RequestError'
  type_array: true
  purpose: 'Errors that occured during the request.'
- name: 'suggestions'
  required: true
  type_json: '[{score: &lt;number&gt;, key: &lt;string&gt;, value: &lt;string&gt;}, ...]'
  purpose: 'Suggested tag key-value combinations'
---
