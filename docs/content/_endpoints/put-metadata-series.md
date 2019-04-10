---
method: PUT
endpoint: /metadata/series
help: Insert time series metadata
description: Write the given series metadata.
fields:
- name: key
  type_json: '&lt;string&gt;'
  purpose: The key of the series.
- name: tags
  type_json: '{&lt;string&gt;: &lt;string&gt;}'
  purpose: The tags of the series.
---
