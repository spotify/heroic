---
method: POST
endpoint: /metadata/tagkey-count
help: Search for tag keys with the count of unique values per tag.
description: Query for  tag keys with value counts
request: metadata-tagkey-count.json
fields:
- name: filter
  required: true
  type_name: Filter
  purpose: A filter to use when quering for tags.
- name: limit
  required: false
  type_name: Limit
  purpose: Limit the response
response_fields:
- name: 'errors'
  type_name: 'RequestError'
  type_array: true
  purpose: 'Errors that occured during the request.'
- name: 'suggestions'
  required: true
  type_json: '[{key: &lt;string&gt; count: &lt;number&gt;,exactValues: &lt;string&gt;}, ...]'
  purpose: 'Suggested tag keys with value counts'
- name: 'limited'
  type_name: Limit
  purpose: 'Was the response limited or not.'
---
