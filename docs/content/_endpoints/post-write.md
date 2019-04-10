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
---
