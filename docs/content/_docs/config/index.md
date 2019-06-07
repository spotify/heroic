---
title: Configuration
---

# Configuration

Heroic is divided up into a few components, each whose configuration will affect the service in different ways. Each component has its own sub-section in the configuration file.

## Configuration file

The configuration for Heroic is loaded from [a YAML file](https://en.wikipedia.org/wiki/YAML) at runtime. A valid example file can be found [in the example directory on GitHub](https://github.com/spotify/heroic/blob/master/example/heroic.example.yml).

```yaml
# Port to listen on for the admin API.
port: <int>

# Clustering and federation to support global interfaces.
cluster:
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

# Persistent storage of metrics.
metrics:
  # Metric backend configurations.
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

  # hreshold for defining a "small" query, measured in pre-aggregation sample size.
  smallQueryThreshold: <int> default = 200000

# Metadata backends responsible for indexing the active time
# series to support filtering.
metadata: {}

# Suggest backends that provide feedback on which tags
# and time series are available.
suggest: {}

# Consumers that ingest raw data.
consumers: []

# Caching for aggregations.
cache: {}

# HTTP client configuration.
client: {}

# Detailed query logging
queryLogging:
  type: slf4j

# Distributed tracing output.
tracing: {}
```

### `<discovery_config>`

The mechanism used to discover nodes in the cluster. Only one disovery type can be set at a time.

#### static

Static is the simplest possible form of discovery. It takes a list of nodes that may, or may not be reachable at the moment.
This list will be queried at a given interval, and any that responds to a metadata request will be added to the local list of known members.

```yaml
type: static

# The list of nodes that this node will attempt to add to its cluster registry. A valid url has the form protocol://host[:port], eg grpc://localhost:8698
nodes:
  - <string>
  - ...
```

#### srv

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

### `<protocol_config>`

Protocols that the node can speak to other nodes. Multiple protocols can be enabled at once.

#### grpc

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

#### jvm

Communicate directly between multiple JVMs running on the same host.

```yaml
type: jvm

# Unique name for this JVM instance.
bindName: <string> default = heroic-jvm
```

### `<metrics_backend>`

The metric backends are responsible for storing and fetching metrics to and from various data stores.

#### Cassandra

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
