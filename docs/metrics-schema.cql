CREATE TABLE metrics (
  metric_key blob,
  data_offset blob,
  data_value double,
  PRIMARY KEY(metric_key, data_offset)
) WITH COMPACT STORAGE;
