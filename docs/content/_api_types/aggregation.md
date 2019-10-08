---
type_name: Aggregation
fields:
- name: type
  required: true
  type_json: '"relative"'
  purpose: Type of the aggregation.
- name: sampling
  type_name: SamplingQuery
  purpose: Sampling to use with aggregation.
---
Java Class: {% include github-link.html name='aggregation.Aggregation' %}

An aggregation is responsible for analysing and sampling a larger dataset into a smaller, more manageable one.
For details on all available aggregations, see the [Aggregations Section]({{ "/docs/aggregations" | relative_url }}).

This object tells the distance to the point in the past.
