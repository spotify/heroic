---
title: Release Notes
---

# {{ page.title }}

The biggest updates for Heroic are highlighted below. For a full list of changes, see the relevant release [on GitHub](https://github.com/spotify/heroic/releases).

Changes listed for future releases are expectations but not hard commitments. The roadmap may be updated at any time.

### 1.2 (planned)

Theme: Metric storage improvements.

[Tracker](https://github.com/spotify/heroic/milestone/3)

- **BREAKING** Text-based serialization for BigTable RowKeys, allowing for better integration with standard BigTable tools.
- Retention policy automatically set on BigTable clusters.
- New timeseries sampled and derived from raw data upon ingestion.
- Configurable options for BigTable partitioning.

### 1.1 (planned)

Theme: Advanced query usability.

[Tracker](https://github.com/spotify/heroic/milestone/2)

- Aggregations for rate of change.
- Processing of arithmetic queries.
- Query clients for Java and Python.
- Better support for querying with a non-JSON DSL.

### 1.0 (planned)

Theme: Stability and documentation.

[Tracker](https://github.com/spotify/heroic/milestone/1)

- **BREAKING** Removal of support for events. Attempting to send events (or any non-metric data) through the ingestion pipeline will generate an error.
- Documentation overhaul, complete with architectural references.
- Java 11 compatability.
- Analytics added on startup to track deployments and usage. Analytics can be disabled with a config option.

### 0.9

[Released 2019-04-30](https://github.com/spotify/heroic/releases/tag/0.9.0)

- **BREAKING** Update semantic-metrics for internal instrumention, replacing meters with counters.
- Use protobuf for shell serialization.
- **BREAKING** Remove deprecated key-value v1 metadata backend. ElasticSearch is still fully supported as the MetadataBackendKV module.
- Enable dynamic ElasticSearch node lookup.
