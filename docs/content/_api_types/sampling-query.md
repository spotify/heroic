---
type_name: SamplingQuery
fields:
  - name: unit
    type_json: '&lt;string&gt;'
    purpose: The unit to use when building the sampling bucket. E.g. `seconds`, `minutes`, `hours`.
  - name: value
    required: true
    type_json: '&lt;number&gt; | 120'
    purpose: The value/size of the sample.
    
---
A complete example is shown below.

`"sampling": {"unit": "seconds", "value": 120}`

<a href="https://github.com/spotify/heroic/blob/master/heroic-component/src/main/java/com/spotify/heroic/aggregation/SamplingQuery.kt">com.spotify.heroic.aggregation.SamplingQuery</a>
