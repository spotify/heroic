# Heroic Metrics API

Heroic is a service to make a simplified and safe API on top of metrics and
event databases.

###### Documentation

+ [API documentation](docs/api.md)

# Usage

Build the project.

```shell
#> mvn clean package
```

# Database Setup

## Setting Up ElasticSearch

Make sure there is no pre-existing metadata index.

```sh
curl -X DELETE http://<url>/heroic/metadata
```

Setup new mapping.

```sh
curl -d @docs/metadata-mapping.json http://<url>/heroic/metadata/_mapping
```

## Preparing Cassandra

Cassandra needs to be configured with a keyspace and tables (CFs) for metrics
and aggregations.

```
cqlsh < docs/keyspace.cql
cqlsh -k heroic < docs/aggregation-schema.cql
cqlsh -k heroic < docs/metrics-schema.cql
```
