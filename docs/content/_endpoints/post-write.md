---
method: POST
endpoint: /write
help: Write metrics
description: Used for writing data into heroic directly.
fields:
  - name: series
    required: true
    type_name: Series
    purpose: Time series to write data to.
  - name: data
    required: true
    type_name: MetricCollection
    purpose: Collection of metrics to write.
response_fields:
  - name: errors
    type_name: RequestError
    type_array: true
    purpose: 'Potential errors returned either from different shards or for specific time series. The presence of an error does not cause the entire query to fail, instead it is up to the client to use this information to decide if the response is reliable enough.'
  - name: times
    type_json: '[&ltnumber&gt, ...]'
    purpose: 'Timing information for the write. Each entry indicates the amount of time it took a shard to perform the write, in nanoseconds.'
---
