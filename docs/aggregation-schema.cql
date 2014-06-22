CREATE TABLE aggregations_1200 (
  aggregation_key blob,

  data_offset int,
  data_value double,
  data_p float,
  PRIMARY KEY(aggregation_key, data_offset)
);
