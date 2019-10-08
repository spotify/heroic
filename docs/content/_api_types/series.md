---
type_name: Series
fields:
  - name: key
    required: true
    type_json: '&lt;string&gt;'
    purpose: Key of the series.
  - name: tags
    required: true
    type_json: '{&lt;string&gt;: &lt;string&gt;, ...}'
    purpose: Tags of the series.
---
{% include github-link.html module='heroic-component' name='common.Series' %}


See the [Series section in Data Model]({{ "/docs/data_model#series" | relative_url }})
for details about <em>what</em> a series is.
