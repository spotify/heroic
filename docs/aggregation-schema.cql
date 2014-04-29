CREATE TABLE aggregations_1200 (
  aggregation_key blob,

  data_offset int,
  data_value double,
  PRIMARY KEY(aggregation_key, data_offset)
);
