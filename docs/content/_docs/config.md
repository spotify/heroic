---
title: Configuration
---

{::options toc_levels="3..4" /}

# Configuration

Heroic is divided up into a few components, each whose configuration will affect the service in different ways. Each component has its own sub-section in the configuration file.

* TOC
{:toc}

## Configuration file

The configuration for Heroic is loaded from [a YAML file](https://en.wikipedia.org/wiki/YAML) at runtime. A valid example file can be found [in the example directory on GitHub](https://github.com/spotify/heroic/blob/master/example/heroic.example.yml).

```yaml
# id, version, and service are used as metadata for the node.
id: <string> default = heroic
version: <string> default = loaded from build artifact
service: <string> default = The Heroic Time Series Database

# The time to wait for all services to start/stop before throwing an exception.
startTimeout: <duration> default = 5m
stopTimeout: <duration> default = 1m

# Host and port to listen on for the HTTP API.
host: <string> default = 0.0.0.0
port: <int> default = 8080

# Enable CORS for the HTTP API.
enableCors: <bool> default = true

# Allowed CORS origin for the HTTP API. Has no effect unless enableCors is true.
corsAllowOrigin: <string> default = "*"

# Flags used to enable/disable features.
features:
  - <feature>
  - ...

# Used to enable/disable feature flags based on request parameters.
conditionalFeatures:
  type: list
  list:
    - type: match
      features:
        - <feature>
        - ...
      condition: <feature_request_condition>
    - ...

# Clustering and federation to support global interfaces.
cluster: <cluster_config>

# Persistent storage of metrics.
metrics: <metrics_config>

# Metadata backends responsible for indexing the active time
# series to support filtering.
metadata:
  backends:
    - <metadata_backend>
    ...
  # A list of backend group names that are part of the default group. The default group is the group of backends
  # that are used for operations unless a specified group is used.
  defaultBackends: default = all configured backends
    - <string>
    - ...

# Suggest backends that provide feedback on which tags
# and time series are available.
suggest:
  backends:
    - <suggest_backend>
    ...
  # A list of backend group names that are part of the default group. The default group is the group of backends
  # that are used for operations unless a specified group is used.
  defaultBackends: default = all configured backends
    - <string>
    - ...

# List of consumers that ingest raw data.
consumers:
  - <consumer_config>
  - ...

# Caching for aggregations.
cache:
  backend: <cache_backend>

# Binding settings for the Heroic shell server. This allows full control over the Heroic node and should
# be restricted.
shellServer:
  host: <string> default = localhost
  port: <int> default = 9190

# Optionally store analytic data about processed queries.
analytics: <analytics_config>

# Generate internal metrics from the operation of Heroic.
# Disabled by defaults, the only valid option is semantic.
statistics:
  type: semantic

# Detailed query logging.
queryLogging: <query_logging_config>

# Enable distributed tracing of Heroic's operations.
tracing: <tracing_config>
```

### [`<feature>`](#feature)

Features are a way to modify the behaviour of the service. They are implemented as flags namespaced to Heroic.

Features can be configured either on a per-query basis, or in the configuration section `features` to apply it to all queries by default. They can either be enabled or disabled. To enable a flag, you specify its name. To disable it, you specify it's name prefixed with a minus sign: `-<feature>`.

Precedence for each flag is defined as the following:

- Query
- Configuration
- Default

The following features are available:

#### com.spotify.heroic.deterministic_aggregations
{:.no_toc}

Enable feature to only perform aggregations that can be performed with limited resources. Disabled by default.

Aggregations are commonly performed per-shard, and the result concatenated. This enabled experimental support for distributed aggregations which behave transparently across shards.

#### com.spotify.heroic.distributed_aggregations
{:.no_toc}

Enable feature to perform distributed aggregations. Disabled by default.

Aggregations are commonly performed per-shard, and the result concatenated. This enables experimental support for distributed aggregations which behave transparently across shards. Typically this will cause more data to be transported across shards for each request.

#### com.spotify.heroic.shift_range
{:.no_toc}

Enable feature to cause range to be rounded on the current cadence. Enabled by default.

This will assert that there are data outside of the range queried for and that the range is aligned to the queried cadence. Which is a useful feature when using a dashboarding system.

#### com.spotify.heroic.sliced_data_fetch
{:.no_toc}

Enable feature to cause data to be fetched in slices. Enabled by default.

This will cause data to be fetched and consumed by the aggregation framework in pieces avoiding having to load all data into memory before starting to consume it.

#### com.spotify.heroic.end_bucket_stategy
{:.no_toc}

Enabled by default.

Use the legacy bucket strategy by default where the resulting value is at the end of the timestamp of the bucket.

#### com.spotify.heroic.cache_query
{:.no_toc}

Disabled by default.

Permit caching of the query results.

### [`<feature_request_condition>`](#feature_request_condition)

Features can be conditionally enabled and disabled by matching properties of the request. Specific conditions can be combined using the `all` or `any` conditions shown below.

`all` matches if any child condition match.

```yaml
type: any
conditions:
  - <feature_request_condition>
  - ...
```

`any` matches if all of the child condition match.

```yaml
type: any
conditions:
  - <feature_request_condition>
  - ...
```

`clientId` match a given client id. Only exact matches are supported.

```yaml
type: clientId
clientId: <string>
```

`userAgent` matches against the request user agent. Only exact matches are supported.

```yaml
type: userAgent
userAgent: <string>
```

### [`<cluster_config>`](#cluster_config)

Configuration related to the cluster of Heroic nodes.

```yaml
# Local ID for the node.
id: <string> default = a generated UUID

# When communicating with self, avoid using the network.
useLocal: <bool> default = false

# The mechanism used to discover nodes in the cluster.
discovery: <discovery_config>

# Protocols that that node can use to communicate with other Heroic nodes.
protocols:
  - <protocol_config>
  - ...

# Defines a set of tags that identifies which part of the cluster this node belongs to.
# Nodes that have an identical set of tags are said to be part of the same shard.
# See the federation section for more details.
tags:
  <string>: <string>
  ...

# Actual topology (shards) is detected based on the metadata coming from the nodes.
# Expected topology is specified in the optional 'topology' list. This specifies the minimum
# shards expected, i.e. additional shards may also exist. If an expected shard is not responding the
# response will include which shard was failing. See the federation section for more details.
topology:
  - <string>: <string>
  - ...
```

### [`<discovery_config>`](#discovery_config)

The mechanism used to discover nodes in the cluster. Only one disovery type can be set at a time.

#### [static](#static)

Static is the simplest possible form of discovery. It takes a list of nodes that may, or may not be reachable at the moment.
This list will be queried at a given interval, and any that responds to a metadata request will be added to the local list of known members.

```yaml
type: static

# The list of nodes that this node will attempt to add to its cluster registry. A valid url has the form protocol://host[:port], eg grpc://localhost:8698
nodes:
  - <string>
  - ...
```

#### [srv](#srv)

SRV records are useful when running Heroic on more ephemeral environments, like Kubernetes.
It takes a list of SRV records and resolves each one at a given interval. Any that respond to a metadata
request will be added to the local list of known members.

```yaml
type: srv

# The list of srv records that this node will attempt to resolve and add  to its cluster registry.
records:
  - <string>
  - ...

# Protocol to use when building the URI.
protocol: <string> default = grpc

# Port to use when building the URI. Used to override the port returned by the SRV lookup.
port: <int> default = result from SRV
```

### [`<protocol_config>`](#protocol_config)

Protocols that the node can speak to other nodes. Multiple protocols can be enabled at once.

#### [grpc](#grpc)

[gRPC](https://grpc.io) is an open source RPC protocol.

```yaml
type: grpc

# The address of the interface that this node will bind to.
host: <string> default = 0.0.0.0

# The port number that this node will bind to.
port: <int> default = 9698

# Frame size limit in bytes.
maxFrameSize: <int> default = 10000000
```

#### [jvm](#jvm)

Communicate directly between multiple JVMs running on the same host. This is generally only used for testing purposes.

```yaml
type: jvm

# Unique name for this JVM instance.
bindName: <string> default = heroic-jvm
```

### [`<metrics_config>`](#metrics_config)

Configuration for reading and writing metrics. At least one backend must be configured for the node to interact with metrics.

```yaml
backends:
  - <metrics_backend>
  ...
# A list of backend group names that are part of the default group. The default group is the group of backends
# that are used for operations unless a specified group is used.
defaultBackends: default = all configured backends
  - <string>
  - ...

# Maximum number of distinct groups a single result group may contains.
groupLimit: <int>

# Maximum amount of time series a single request is allowed to fetch.
seriesLimit: <int>

# Maximum number of data points a single aggregation is allowed to output.
aggregationLimit: <int>

# Maximum number of data points a single request may fetch from the backends.
dataLimit: <int>

# Limit how many concurrent queries that the MetricManager will accept. When this level is
# reached, the result will be back-off so that another node in the cluster can be used instead.
concurrentQueriesBackoff: <int>

# How many fetches are allowed to be performed in parallel for each request.
fetchParallelism: <int> default = 100

# When true, any limits applied will be reported as a failure.
failOnLimits: <bool> default = false

# Threshold for defining a "small" query, measured in pre-aggregation sample size.
smallQueryThreshold: <int> default = 200000
```

### [`<metrics_backend>`](#metrics_backend)

The metric backends are responsible for storing and fetching metrics to and from various data stores.

#### [Memory](#memory)

An in-memory datastore. This is intended only for testing and is definitely not something you should run in production.

```yaml
type: memory

# ID used to uniquely identify this backend.
id: <string> default = generated UUID

# Which groups this backend should be part of.
groups:
  - <string> default = memory
  ...

# If true, synchronized storage for happens-before behavior.
synchronizedStorage: <bool> default = false
```

#### [Cassandra](#cassandra)

```yaml
type: datastax

# ID used to uniquely identify this backend.
id: <string> default = generated UUID

# Which groups this backend should be part of.
groups:
  - <string> default = heroic
  ...

# A list of seed hosts to use when connecting to a C* cluster.
seeds:
  - <string> default = localhost
  - ...

# Data schema to use.
schema:
  # The type of schema to use, either ng or legacy. `ng` stands for next generation and should be used for all new installations.
  # The initial legacy schema was inherited in part from some of the key composition utilities
  # made available by datastax, making it a bit awkward to work with for other utilities.
  type: [ng | legacy] default = ng
  # Name of the Cassandra keyspace to use.
  keyspace: <string> default = heroic

# Automatically configure the database.
configure: <bool> default = false

# The default number of rows to fetch in a single batch.
fetchSize: <int> default = 5000

# The default read timeout for queries
readTimeout: <duration> default = 30s

# Default consistency level to use for reads and writes. Set as the enum from com.datastax.driver.core.ConsistencyLevel.
# See https://docs.datastax.com/en/archived/cassandra/3.0/cassandra/dml/dmlConfigConsistency.html for possibile options.
consistencyLevel: <string> default = com.datastax.driver.core.ConsistencyLevel.ONE

# Authentication to apply to the Cassandra connection. Username and password are only used for `plain` authentication.
authentication:
  type: [none | plain] default = none
  username: <string>
  password: <string>

# Client pooling options.
poolingOptions:
  # Maximum number of requests per connection.
  maxRequestsPerConnection: <int> default = 1024 for LOCAL hosts and 256 for REMOTE hosts

  # The core number of connections per host in one call.
  coreConnectionsPerHost: <int> default = 1

  # The maximum number of connections per host in one call.
  maxConnectionsPerHost: <int> default = 1

  # Threshold that triggers the creation of a new connection to a host.
  newConnectionThreshold: <int> default = 800 for LOCAL hosts and 200 for REMOTE hosts

  # Maximum number of requests that get enqueued if no connection is available.
  maxQueueSize: <int> default = 256

  # Timeout when trying to acquire a connection from a host's pool, in milliseconds.
  poolTimeoutMillis: <int> default = 5000

  # Timeout before an idle connection is removed.
  idleTimeoutSeconds: <int> default = 120

  # Interval after which a message is sent on an idle connection to make sure it's still alive.
  heartbeatIntervalSeconds: <int> default = 30
```

#### [Bigtable](#bigtable)

Store metrics in [Google's Cloud Bigtable](https://cloud.google.com/bigtable/).

A note on sending metrics with the same timestamp and/or duplicate metrics. These metric values will not be duplicated within the row, since Heroic is mutating rows and not appending to the column family. In Bigtable each timestamp + value is a column within the row.

```yaml
type: bigtable

# ID used to uniquely identify this backend.
id: <string> default = generated UUID

# Which groups this backend should be part of.
groups:
  - <string> default = bigtable
  ...

# The Google Cloud Project the backend should connect to.
project: <string> required

# The Bigtable instance the backend should connect to.
instance: <string> default = heroic

# Which Bigtable table the backend should use.
table: <string> default = metrics

# Credentials used to authenticate. If this is not set, automatic authentication will be attempted
# as detailed at https://cloud.google.com/docs/authentication/production#finding_credentials_automatically
credentials: <bigtable_credentials> default = automatic discovery

# Automatically configure the database.
configure: <bool> default = false

# This will cause each individual write to be performed as a single request.
disableBulkMutations: <bool> default = false

# When bulk mutations are enabled, this is the maximum amount of time a single batch will collect data for.
flushIntervalSeconds: <int> default = 2

# When bulk mutations are enabled, this is the maximum size of a single batch.
batchSize: <int>

# If set, no actual connections will be made to Bigtable.
fake: <bool> default = false
```

##### `<bigtable_credentials>`

One of the following credential configurations can be set to explicitly define how to authenticate.

```yaml
# Compute Engine Credentials
type: compute-engine
```

```yaml
# Default Credentials
type: default
```

```yaml
# JSON Credentials
type: json

# Path to credentials file to use.
path: <string>
```

### [`<metadata_backend>`](#metadata_backend)

Metadata acts as the index to time series data, it is the driving force behind our [Query Language](docs/query_language).

Metadata resolution is important since it allows operators to specify a subset of known metadata, and resolve it into a set of matching time series. Without metadata, the burden of keeping track of time series would lie solely in the client.

#### [Elasticsearch](#elasticsearch)

Elasticsearch based metadata. Example of the stored metadata:

```json
{'_index': 'heroic-1535587200000', '_type': 'metadata', '_id': '447939eaf69475f685518dc2c179ddaf', '_version': 1, 'found': True, '_source': {'key': 'apollo', 'tags': ['component\x00memcache-client', 'operation\x00get', 'what\x00memcache-results'], 'tag_keys': ['component', 'operation', 'what']}}
```

**WARNING** There are ElasticSearch settings and mappings that must be configured before indexing operations are processed. These are required to make the reads efficient. At Spotify these settings are added when setting up the ElasticSearch cluster with Puppet. [settings/mappings are here](https://github.com/spotify/heroic/blob/7ff07a654048ce760e867835e11f230cd7c5a4ee/metadata/elasticsearch/src/main/resources/com.spotify.heroic.metadata.elasticsearch/kv/metadata.json)

The settings `clusterName`, `seeds`, `sniff`, `nodeSamplerInterval`, and `nodeClient` used to be found under `connection` but are deprecated. The behavior they enabled is now set by configuring an `<es_client_config>` in the `connection`.

```yaml
type: elasticsearch

# ID used to uniquely identify this backend.
id: <string> default = generated UUID

# Which groups this backend should be part of.
groups:
  - <string> default = elasticsearch
  ...

# Settings specific to the Elasticsearch connection.
connection:
  # Elasticsearch index settings.
  index: <es_index_config> default = rotating

  # The Elasticsearch client configuration to use.
  client: <es_client_config> default = transport

# The number of writes this backend allows per second before rate-limiting kicks in.
writesPerSecond: <int> default = 3000

# The duration where the rate limiter ramps up its rate before reaching the rate set in writesPerSecond.
rateLimitSlowStartSeconds: <int> default = 0

# The number of minutes a write will be cached for.
writeCacheDurationMinutes: <int> default = 240

# Guides the allowed concurrency among update operations for the write cache.
writeCacheConcurrency: <int> default = 4

# Specifies the maximum number of entries the write cache may contain.
writeCacheMaxSize: <int> default = 30000000

# SRV record used to lookup a memcached cluster. If set, memcached will be used
# for limiting writes to Elasticsearch in addition to a local in-memory write cache.
distributedCacheSrvRecord: <string> default = empty string

# Concurrent operations allowed when deleting series from Elasticsearch.
deleteParallelism: <int> default = 20

# Default name of the template that will be configured in Elasticsearch for this backend.
templateName: <string> default = heroic-metadata

# Automatically configure the database.
configure: <bool> default = false
```

##### `<es_index_config>`

Index mapping to use.

###### single

```yaml
# Only operate on one index.
type: single

# Name of the index.
index: <string> default = heroic
```

###### rotating

```yaml
# Create new indices over time.
type: rotating

# Interval in milliseconds that each index is valid.
interval: <duration> default = 7d

# Maximum indices to read at a time. Minimum of 1.
maxReadIndices: <int> default = 2

# Maximum indices to write to at a time. Minumum of 1.
maxWriteIndices: <int> default = 1

# Pattern to use when creating an index. The pattern must contain a single '%s' that will be
# replaced with the base time stamp of the index.
pattern: <string> default = heroic-%s
```

##### `<es_client_config>`

The Elasticsearch client configuration to use.

###### standalone

Complete local cluster. This is typically used when running a fully in-memory configuration of Heroic.

```yaml
type: standalone

# The name of the cluster to setup.
clusterName: <string> default = heroic-standalone

# Root directory where indexes will be stored.
# If omitted, will create a temporary root directory.
root: <string>
```

###### node

Join the cluster as a non-data, non-leader node. This can yield better performance since index lookups and aggregations can be performed without having to 'hop' to another node.

However, due the complexity involved in the client this mode is typically recommended against.

```yaml
type: node

# The name of the cluster to setup.
clusterName: <string> default = elasticsearch

# Initial nodes in the cluster to connect to.
seeds:
  - <string>
  ...
```

###### transport

Connect using the transport protocol. This is the most lightweight method of interacting with the Elasticsearch cluster.

```yaml
type: transport

# The name of the cluster to setup.
clusterName: <string> default = heroic-standalone

# Initial nodes in the cluster to connect to. Any hosts without a port specified
# are assumed to use port 9300. Useful to have masters that rarely change as seeds.
seeds:
  - <string> default = localhost:9300
  ...

# Dynamically sniff new nodes.
sniff: <bool> default = false

# How often to poll for new nodes when `sniff` is enabled.
nodeSamplerInterval: <duration> default = 30s
```


#### [Memory](#memory)

An in-memory datastore. This is intended only for testing and is definitely not something you should run in production.

```yaml
type: memory

# ID used to uniquely identify this backend.
id: <string> default = generated UUID

# Which groups this backend should be part of.
groups:
  - <string> default = memory
  ...

# If true, synchronized storage for happens-before behavior.
synchronizedStorage: <bool> default = false
```

### [`<suggest_backend>`](#suggest_backend)

The ability to perform suggestions is an important usability feature. It makes the difference for your system to be a complete black box, to giving your developers the ability to find and make use of time series on their own. Suggests are fairly expensive in terms of data storage and indexing operations as each tag that is part of a metric is indexed.

Suggestions is an optional feature of heroic.

#### [Elasticsearch](#elasticsearch)

**WARNING** There are ElasticSearch settings and mappings that must be configured before indexing operations are processed. These are required to make the reads efficient. At Spotify these settings are added when setting up the ElasticSearch cluster with Puppet. [settings/mappings are here](https://github.com/spotify/heroic/tree/7ff07a654048ce760e867835e11f230cd7c5a4ee/suggest/elasticsearch/src/main/resources/com.spotify.heroic.suggest.elasticsearch/kv).

Example of stored suggestion in Elasticsearch:

```json
{'_index': 'heroic-1536192000000', '_type': 'series', '_id': '447939eaf69475f685518dc2c179ddaf', '_version': 1, 'found': True, '_source': {'key': 'apollo', 'tags': ['component\x00memcache-client', 'operation\x00get', 'what\x00memcache-results'], 'tag_keys': [component', 'operation', 'what'], 'series_id': '447939eaf69475f685518dc2c179ddaf'}}

{'_index': 'heroic-1536192000000', '_type': 'tag', '_id': '447939eaf69475f685518dc2c179ddaf:687d7854', '_version': 1, 'found': True, '_source': {'key': 'apollo', 'tags': ['component\x00memcache-client', 'what\x00memcache-results', 'operation\x00get'], 'tag_keys': ['component', 'what', 'operation'], 'series_id': '447939eaf69475f685518dc2c179ddaf', 'skey': 'component', 'sval': 'memcache-client', 'kv': 'component\x00memcache-client'}}
```

```yaml
type: elasticsearch

# ID used to uniquely identify this backend.
id: <string> default = generated UUID

# Which groups this backend should be part of.
groups:
  - <string> default = elasticsearch
  ...

# Settings specific to the Elasticsearch connection.
connection:
  # Elasticsearch index settings.
  index: <es_index_config> default = rotating

  # The Elasticsearch client configuration to use.
  client: <es_client_config> default = transport

# The number of writes this backend allows per second before rate-limiting kicks in.
writesPerSecond: <int> default = 3000

# The duration where the rate limiter ramps up its rate before reaching the rate set in writesPerSecond.
rateLimitSlowStartSeconds: <int> default = 0

# The number of minutes a write will be cached for.
writeCacheDurationMinutes: <int> default = 240

# Guides the allowed concurrency among update operations for the write cache.
writeCacheConcurrency: <int> default = 4

# Specifies the maximum number of entries the write cache may contain.
writeCacheMaxSize: <int> default = 30000000

# SRV record used to lookup a memcached cluster. If set, memcached will be used
# for limiting writes to Elasticsearch in addition to a local in-memory write cache.
distributedCacheSrvRecord: <string> default = empty string

# Default name of the template that will be configured in Elasticsearch for this backend.
templateName: <string> default = heroic-suggest

# Automatically configure the database.
configure: <bool> default = false
```

#### [Memory](#memory)

An in-memory datastore. This is intended only for testing and is definitely not something you should run in production.

```yaml
type: memory

# ID used to uniquely identify this backend.
id: <string> default = generated UUID

# Which groups this backend should be part of.
groups:
  - <string> default = memory
  ...
```

### [`<consumer_config>`](#consumer_config)

A consumer is a component responsible for ingesting metrics and introducing them into a Heroic cluster.

#### [Kafka](#kafka)

A Kafka consumer that reads and parses data out of a Kafka queue.

```yaml
type: kafka

# ID used to uniquely identify this backend.
id: <string> default = generated UUID

# The schema to use when decoding messages. Expected to be a class name
# that implements com.spotify.heroic.consumer.ConsumerSchema
# Possible options:
#   com.spotify.heroic.consumer.schemas.Spotify100 - JSON based schema
#   com.spotify.heroic.consumer.schemas.Spotify100Proto - Protocol buffer based schema
schema: <string> required

# A list of topics to read from.
topics:
  - <string>
  ...

# Number of threads to use for each topic when consuming.
threadsPerTopic: <int> default = 2

# An object that will be provided to the Kafka consumer as configuration.
# See the official documentation for what is expected:
# https://kafka.apache.org/08/configuration.html#consumerconfigs
config: {}

# If enabled, consumer offsets will be committed periodically. All threads are paused so there are no in-progress
# requests while the commit is occurring.
transactional: <bool> default = false

# How often to commit the offets when `transactional` is enabled, in milliseconds.
transactionCommitInterval: <int> default = 30000
```

#### [PubSub](#pubsub)

Utilize [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs/overview) for ingesting messages.

```yaml
type: pubsub

# ID used to uniquely identify this backend.
id: <string> default = generated UUID

# Number of threads to use for each subscription when consuming.
threadsPerSubscription: <int> default = 8

# The schema to use when decoding messages. Expected to be a class name
# that implements com.spotify.heroic.consumer.ConsumerSchema
# Possible options:
#   com.spotify.heroic.consumer.schemas.Spotify100 - JSON based schema
#   com.spotify.heroic.consumer.schemas.Spotify100Proto - Protocol buffer based schema
schema: <string> required

# The Google Cloud Project the backend should connect to.
project: <string> required

# The PubSub topic where messages are being published. If it does not exist, Heroic will attempt to create it.
topic: <string> required

# The PubSub subscription to consume from. If it does not exist, Heroic will attempt to create it.
subscription: <string> required

# Maximum messages a consumer can read off a subscription before acking them.
maxOutstandingElementCount: <int> default = 20000

# Maximum amount of bytes a consumer can read off a subscription before acking.
maxOutstandingRequestBytes: <int> default = 1000000000

# The maximum message size allowed to be received on the channel. The PubSub API has a limit of 20MB, so this
# value cannot exceed that.
maxInboundMessageSize: <int> default = 20971520

# The time without read activity before sending a keepalive ping.
keepAlive: <int> default = 300
```

### [`<cache_backend>`](#cache_backend)

Caching for aggregations. By default no cache is enabled.

#### Memory

An in-memory only cache.

```yaml
type: memory
```

#### Memcached

Cache in a distributed memcached cluster.

```yaml
type: memcached

# List of addresses of memcached nodes
addresses:
  - <string> default = localhost:11211
  - ...

# Maximum time that a value should be cached.
maxTtl: <duration>
```

### [`<analytics_config>`](#analytics_config)

Configure a backend to store analytics about queries served by Heroic. Currently Bigtable is the only supported backend.

```yaml
type: bigtable

# The Google Cloud Project the backend should connect to.
project: <string> required

# The Bigtable instance the backend should connect to.
instance: <string> default = heroic

# Credentials used to authenticate. If this is not set, automatic authentication will be attempted
# as detailed at https://cloud.google.com/docs/authentication/production#finding_credentials_automatically
credentials: <bigtable_credentials> default = automatic discovery

# Limit the number of pending reports that are allowed at the same time to avoid resource
# starvation. Simultaneous reports above this threshold will be dropped.
maxPendingReports: <int> default = 1000
```

### [`<query_logging_config>`](#query_logging_config)

Defines which type of logger that should be used for detailed query logging. Currently only slf4j is supported.

```yaml
type: slf4j

# Defines the Slf4j logger to use when logging. A matching logger needs to be defined
# in the Slf4j configuration file to actually get some output.
name: <string> default = com.spotify.heroic.query_logging

# Level to log at. From most verbose to least, the possible options are:
# TRACE, DEBUG, INFO, WARN, ERROR
level: <string> default = TRACE
```

#### Query log output
{:.no_toc}

Each successful query will result in several output entries in the query log. Entries from different stages of the query. Example output:

```json
{
  "component": ...,
  "queryId": "ed6fe51c-afba-4320-a859-a88795c15175",
  "clientContext": {
    "dashboardId": "my-system-metrics",
    "user": "my-user"
  },
  "type": ...,
  "data": ...
}
```

| Field | Description
| --- | ---
| `component` | Specifies the internal component in Heroic that outputs this query log output.
| `queryId` | Generated id that is unique for this particular query. Can be used to group query log entries together. The queryId is also returned in the final query response.
| `clientContext` | The contextual information supplied by user. See the Contextual Information section below.
| `type` | Specifies the query stage at which this particular query log entry was generated.
| `data` | Contains data relevant to this query stage. This might for example be the original query, a partial response or the final response.

#### Contextual information
{:.no_toc}

It's possible to supply contextual information in the query. This information will then be included in the query log, to ease mapping of performed query to the query log output.

Add the following clientContext snippet to the query:

```json
{
  "clientContext": {
    "dashboardId": "my-system-metrics",
    "user": "my-user"
  }
  "filter": ...
}
```

You'll get the following output in the query log:

```json
{
  "component": ...,
  "queryId": "ed6fe51c-afba-4320-a859-a88795c15175",
  "clientContext": {
    "dashboardId": "my-system-metrics",
    "user": "my-user"
  },
  "type": ...,
  "data": ...
}
```

### [`<tracing_config>`](#tracing_config)

Enable distributed tracing output of Heroic's operations. Tracing is instrumented using [OpenCensus](https://opencensus.io/).

```yaml
# Probability, between 0.0 and 1.0, of sampling each trace.
probability: <float> default = 0.01

# Local port to expose zpages on. Traces are accessible at http://localhost:{port}/tracez
zpagesPort: <int>

# Configuration for exporting traces to LightStep.
lightstep:

  # Collector host and port running the Lightstep satellite.
  collectorHost: <string> required
  collectorPort: <int> default = 80

  # Lightstep access token
  accessToken: <string> optional

  # Component name will set the "service" name in the Lightstep UI
  componentName: <string> default = heroic

  # Reporting interval in milliseconds.
  reportingIntervalMs: <int> default = 1000

  # Max buffered spans
  maxBufferedSpans: <int> default = 1000

  # If enabled, the client connection will be reset at regular intervals.
  # Used to load balance on client side.
  resetClient: <bool> default = false
```
