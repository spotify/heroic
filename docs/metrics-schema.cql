CREATE TABLE metrics (
  metric_key blob,
  data_timestamp timestamp,
  data_value double,
  PRIMARY KEY(metric_key, data_timestamp)
) WITH COMPACT STORAGE;
