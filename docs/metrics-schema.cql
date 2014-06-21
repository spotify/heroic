CREATE TABLE metrics (
  metric_key blob,
  data_offset int,
  data_value double,
  PRIMARY KEY(metric_key, data_offset)
) WITH COMPACT STORAGE;
