---
type_name: Point
fields:
  - name: datapoint
    required: true
    type_json: '[&lt;timestamp&gt;, &lt;value&gt;]'
    purpose: |
      <p>A single datapoint.</p>
      <p>The <code class="language-json">&lt;timestamp&gt;</code> is the number of milliseconds since unix epoch.</p>
      <p>The <code class="language-json">&lt;value&gt;</code> is the sample value.</p>
---
Java Class: {% include github-link.html module='heroic-component' name='metric.Point' %}

See the [Points section in Data Model](docs/data_model#data-points)
for more details about <em>what</em> a point is.
