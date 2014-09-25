# Heroic Metrics API

Heroic is a service to make a simplified and safe API on top of metrics and
event databases.

###### Documentation

+ [API](docs/api.md)
+ [API Structure (how to read the API documentation)](docs/api-structure-docs.md)

# Usage

Build the project.

```shell
#> mvn clean package
```

# Database Setup

## Preparing Cassandra

Cassandra needs to be configured with a keyspace and tables (CFs) for metrics
and aggregations.

```
cqlsh < docs/keyspace.cql
cqlsh -k heroic < docs/aggregation-schema.cql
cqlsh -k aggregations < docs/metrics-schema.cql
```
