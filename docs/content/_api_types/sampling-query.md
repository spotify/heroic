---
type_name: SamplingQuery
fields:
  - name: unit
    type_json: '&lt;string&gt;'
    purpose: The default unit to use in size and extent (unless specified as shown below). E.g. `seconds`, `minutes`, `hours`.
  - name: size
    required: true
    type_json: '&lt;number&gt; | 300s' 
    purpose: The size to use in a given aggregation. Time unit defaults to seconds if not appended to the end or specified in unit. 
  - name: extent
    required: true
    type_json: '&lt;number&gt; | 300s'
    purpose: The extent to use in a given aggregation.
---
<a href="https://github.com/spotify/heroic/blob/master/heroic-component/src/main/java/com/spotify/heroic/aggregation/SamplingQuery.kt">com.spotify.heroic.aggregation.SamplingQuery</a>
