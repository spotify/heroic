---
type_name: SamplingQuery
fields:
  - name: unit
    type_json: '&lt;string&gt;'
    purpose: The default unit to use in size and extent (unless specified).
  - name: size
    required: true
    type_json: '&lt;number&gt; | {"unit": &lt;string&gt;, "value": &lt;number&gt;}'
    purpose: The size to use in a given aggregation.
  - name: extent
    required: true
    type_json: '&lt;number&gt; | {"unit": &lt;string&gt;, "value": &lt;number&gt;}'
    purpose: The extent to use in a given aggregation.
---
Java Class: {% include github-link.html module='heroic-component' name='aggregation.SamplingQuery' %}
