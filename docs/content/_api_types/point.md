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
<a href="https://github.com/spotify/heroic/blob/master/heroic-component/src/main/java/com/spotify/heroic/metric/Point.kt">com.spotify.heroic.metric.Point</a>

See the [Points section in Data Model]({{ "/docs/data_model#data-points" | relative_url }})
for more details about <em>what</em> a point is.
