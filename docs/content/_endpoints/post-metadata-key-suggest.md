---
method: POST
endpoint: /metadata/key-suggest
help: Search for key suggestions
description: Search for key suggestions. This endpoint is intended for use with auto-completion.
fields:
- name: key
  type_json: '&ltstring&gt;'
  purpose: Key to help limit down suggestions, this is a free text value that is analyzed by the search engine.
- name: filter
  type_name: Filter
  purpose: Filter to apply
- name: range
  type_name: QueryDateRange
  purpose: The time range for which to get suggestions.
- name: limit
  type_json: '&ltnumber&gt;'
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
  type_json: '[{score: &lt;number&gt;, key: &lt;string&gt;}, ...]'
  purpose: 'Suggest key values with scores.'
---
