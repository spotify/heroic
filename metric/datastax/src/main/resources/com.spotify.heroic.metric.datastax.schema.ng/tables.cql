CREATE TABLE IF NOT EXISTS {{keyspace}}.metrics (
  metric_key blob,
  data_timestamp_offset int,
  data_value double,
  PRIMARY KEY(metric_key, data_timestamp_offset)
) WITH COMPACT STORAGE;