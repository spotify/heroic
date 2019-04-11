---
type_name: MetricCollection
fields:
  - name: type
    required: true
    type_json: '"points" | "events"'
    purpose: The type of data to write.
  - name: data
    required: true
    type_json: '[Point | Event, ...]'
    purpose: The data to write. The type depends on the value of the <code class="language-json">type</code> field.
---
Java Class: {% include github-link.html module='heroic-component' name='metric.MetricCollection' %}
