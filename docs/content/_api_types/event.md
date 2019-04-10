---
type_name: Event
layout: api-type-structure
fields:
  - name: datapoint
    required: true
    type_json: '[&lt;timestamp&gt;, {&lt;payload&gt;}]'
    purpose: |
      <p>A single event.</p>
      <p>The <code class="language-json">&lt;timestamp&gt;</code> is the number of milliseconds since unix epoch.</p>
      <p>The <code class="language-json">&lt;payload&gt;</code> is the sample payload.</p>

---
Java Class: {% include github-link.html name='metric.Event' %}

See the [Events section in Data Model](docs/data_model#events)
for more details about <em>what</em> an event is.
