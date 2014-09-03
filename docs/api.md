The following sections document API endpoints for heroic.

Go to [API Structure Documentation](api-structure-docs.md) for information on how to read it.

# Querying for Metrics

This details the API endpoints that are used for querying for data out of Heroic.

### GET /status

Query for the status of a heroic instance.

The status code <code>503</code> is used to indicate to load balancers that a
service is not available for requests right now.

+ Response ```200``` (application/json) [StatusResponse](#statusresponse)
+ Response ```503``` (application/json) [StatusResponse](#statusresponse)
 + Used to indicate to load-balancers that this node is unsuitable to receive requests.
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)

### POST /query/metrics (alias: /metrics) [MetricsRequest](#metricsrequest)

The simplest query method, expects a [MetricsRequest](#metricsrequest), and will return a [MetricsResponse](#metricsresponse) object when successful.

+ Response ```200``` (application/json) [MetricsResponse](#metricsresponse)
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)

# Writing Metrics

### POST /write/metrics [WriteMetrics](#writemetrics)

Used for writing data to Heroic.
Expects a [WriteMetrics](#writemetrics) object.

+ Response ```200``` (application/json) [WriteMetricsResponse](#writemetricsresponse)
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)

# RPC Endpoints

These endpoints are used internally by the cluster.

All other RPC endpoints except ```/rpc/metadata``` are versioned.
The versioned endpoints differ, but can all be found under ```/rpc<version>``` where ```version``` is a nodes provided version.

### GET /rpc/metadata

Query a node for its metadata.

The metadata is used to determine which _shard_ a node belongs to through its ```tags``` attributes.
It is also used to determine which ```capabilities``` a node has which changes which type of requests it can receive.
Finally it provides a ```version``` which allows other node to determine how this node should be communicated with, and which features it support.

+ Response ```200``` (application/json) [RpcMetadata](#rpcmetadata)
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)

###### Example CURL

```sh
curl -H "Content-Type: application/json" http://heroic/write/metrics -d '{"series": {"key": "heroic-test", "tags": {"site": "lon"}}, "data": [[1409775940000, 42.0], [1409775910000, 20.2]]}'
```

# Common Types

The following are structure documentation for common (shared) types in this documentation.

### ErrorMessage

Contains a human readable error that can be returned by the API.

Is typically returned by ```4xx``` and ```5xx``` HTTP status code family responses.

###### Structure
```yml
ErrorMessage:
  # A message describing the error.
  message: required String
```

###### Example
```json
{"message": "Something gone doofed!"}
```

# RPC Types

### RpcMetadata

This object describes how other nodes _can_ communicate with this node.

The following are the available capabilities and their corresponding meaning.

+ ```QUERY``` - Means that the node can receive queries.
+ ```WRITE``` - Means that the node can receive writes.

###### Structure

```yml
NodeCapability: "QUERY" | "WRITE"

RpcMetadata:
  version: required Number
  id: required String
  tags: required {String: String, ..}
  capabilities: required [NodeCapability, ..]
```

###### Example

```json
{"version": 2, "id": "431f3900-33be-11e4-8c21-0800200c9a66", "tags": {"site": "lon"}, "capabilities": ["QUERY"]}
```

# Types

The following are structure documentation for all available types in the API.

### WriteMetrics

A request to write data to a single time series.

###### Structure

See also;

+ [Series](#series)
+ [DataPoint](#datapoint)

```yml
WriteMetrics:
  series: required Series
  data: [DataPoint, ..]
```

###### Example

```json
{
  "series": {"key": "foo", "tags": {"site": "lon", "host": "www.example.com"}},
  "data": [[1300000000000, 42.0], [1300001000000, 84.0]]
}
```


### WriteMetricsResponse

Indicates if a write was successfully queued or not.

###### Structure
```yml
WriteMetricsResponse:
  # true if write was successful, false otherwise.
  ok: required Boolean
```

### Series

The identity of a time series.

###### Structure

```yml
Series:
  # The namespace of a time series.
  key: required String
  # The tags of a time series.
  tags: required {String: String, ..}
```

###### Example

```json
{"key": "foo", "tags": {"site": "lon", "host": "www.example.com"}}
```

### StatusResponse

Contains the status for a specific heroic instance.

###### Structure

```yml
Consumer:
  # Indicates if all the consumer instances are 'ok'.
  ok: required Boolean
  # How many consumer instances are available.
  available: required Number
  # How many consumer instances are ready.
  ready: required Number
  # How many consumer errors have been encountered (in total).
  errors: required Number

Backend:
  # Indicates if all backend instances are 'ok'.
  ok: required Boolean
  # How many backend instances are available.
  available: required Number
  # How many backend instances are ready.
  ready: required Number

MetadataBackend:
  # Indicates if all metadata backend instances are 'ok'.
  ok: required Boolean
  # How many metadata backend instances are available.
  available: required Number
  # How many metadata backend instances are ready.
  ready: required Number

Cluster:
  # Indicates the heroic cluster is 'ok'.
  ok: required Boolean
  # How many nodes that are considered 'online' and available.
  onlineNodes: required Number
  # How many nodes that are considered 'offline' and unavailable.
  offlineNodes: required Number

StatusResponse:
  ok: required Boolean
  consumers: required Consumer
  backends: required Backend
  metadataBackends: required MetadataBackend
  cluster: required Cluster
```

### QueryDateRange

Defines a DateRange, but with a much more flexible format.

A QueryDateRange has a __type__ field defining its type.

The following types are available.

+ Absolute - Queries an absolute time range.
+ Relative - Queries a time range which is relative to the current time.

###### Structure
```yml
Absolute:
  type: required "absolute"
  # Starting timestamp in milliseconds from the unix epoch.
  start: required Number
  # Ending timestamp in milliseconds from the unix epoch.
  end: required Number

Relative:
  type: required "absolute"
  # Unit to use for 'value'.
  unit: required "MILLISECONDS"|"SECONDS"|"MINUTES"|"HOURS"|"DAYS"|"WEEKS"|"MONTHS"
  # How many 'unit' timespans back in time this date starts.
  value: required Number
```

###### Examples

```json
{"type": "absolute", "start": 1300000000000, "end": 1400000000000}
```

```json
{"type": "relative", "unit": "HOURS", "value": 8}
```

### DateRange

A date range, starting at __start__ milliseconds from the unich epoch and ending at __end__ milliseconds from the unix epoch.

###### Structure
```yml
DateRange:
  # Starting timestamp in milliseconds from the unix epoch.
  start: required Number
  # Ending timestamp in milliseconds from the unix epoch.
  end: required Number
```

###### Example

```json
{"start": 1300000000000, "end": 1400000000000}
```

### DataPoint

A datapoint is represented as an array with two elements.
The __first__ element is the timestamp which is milliseconds from the unix epoch.
The __second__ element is the value.

###### Structure
```yml
DataPoint: [Number, Number]
```

###### Example

```json
[1300000000000, 42.0]
```

### Statistics

Comprehensive statistics about the executed query.

This object contains tons of valuable information that should be analyzed on each request to get an idea of how __valid__ the result is.

###### Structure
```yml
Statistics:
  # Aggregator statistics for a query.
  aggregator:
    #  How many original datapoints were involved to perform the required
    # aggregates.
    sampleSize: required Number
    # How many original datapoints were out of bounds for the specified
    # aggregate.
    # This indicates that heroic has queried unecessary data from the backends.
    outOfBounds: required Number
  # Cassandra row statistics for a query.
  row:
    # How many successful database row was loaded into heroic.
    successful: required Number
    # How many database rows failed to be loaded by heroic.
    failed: required Number
    # How many row fetches were cancelled.
    cancelled: required Number
  # Cache statistics for a query.
  cache:
    # How many resulting data points could be fetched from cache.
    hits: required Number
    # How many cached data points conflicted with each other.
    conflicts: required Number
    # How many calculated data points conflicted with the ones from cache.
    cacheConflicts: required Number
    # How many cached NaN's that were loaded.
    cachedNans: required Number
  # RPC statistics for a query.
  rpc:
    # How many successful RPC requests were executed for this query.
    successful: required Number
    # How many failed RPC requests.
    failed: required Number
    # How many cluster nodes were considered online during the query.
    onlineNodes: required Number
    # How many cluster nodes were considered offline during the query.
    offlineNodes: required Number
```

This statistics object is useful for determining the correctness of the query.

The following are strong indicators that the query has not resulted in correct
data.

+ __row.failed__ is greater than zero.
+ __rpc.failed__ is greater than zero.
+ __rpc.offlineNodes__ is greater than zero.

__cache.cachedNans__ is an indication of _bad_ entries in cache but should not
be considered an error.

If any of these are true, an error should be displayed to the user telling them
that the time series they are seeing is probably inconsistent.

###### Example

```json
{
  "aggregator": {
    "sampleSize": 100,
    "outOfBounds": 0
  },
  "row": {
    "successful": 10,
    "failed": 0,
    "cancelled": 0
  },
  "cache": {
    "hits": 100,
    "conflicts": 0,
    "cacheConflicts": 0,
    "cachedNans": 0
  },
  "rpc": {
    "successful": 3,
    "failed": 0,
    "onlineNodes": 12,
    "offlineNodes": 0
  }
}
```

### Filter

A filter statement is a recursive structure that allows you to define a filter for which time series should be selected for a specific query.

###### Structure

```yml
# Match only if all child filter statements match.
AndFilter: ["and", Filter, ..]

# Match if any child filter statements match.
OrFilter: ["or", Filter, ..]

# Match if child filter statement does not match.
NotFilter: ["not", Filter]

# Match if a time series has the specified key.
MatchKeyFilter: ["key", String]

# Match if a time series has the specified tag and value combination.
MatchTagFilter: ["=", String, String]

# Match if a time series has the specified tag.
HasTagFilter: ["+", String]
```

###### Example

The following example will select time series that the key is either <code>foo</code> or <code>bar</code>, the <code>host</code> tag is set to <code>foo.example.com</code>, the <code>role</code> tag is set to <code>example</code>, a tag called <code>site</code> is present, and that the <code>site</code> tag is _not_ equal to <code>lon</code>. 

```json
["and",
  ["or", ["key", "foo"], ["key", "bar"]],
  ["=", "host", "foo.example.com"],
  ["=", "role", "example"],
  ["+", "site"],
  ["not", ["=", "site", "lon"]]
]
```

### QuerySampling

Defines a sampling period and an extent.

###### Structure

```yml
QuerySampling:
  # The time unit to use for the below fields.
  unit: required TimeUnit
  # The size of the sampling period to use in accordance with the given unit.
  size: required Number
  # The extent for which the request should get data for each sampling period
  # in accordance with the given time unit.
  # Defaults to the same as size.
  extent: optional Number
```

###### Example

The following example would define an extent with a sampling period of
<code>10</code> minutes, which would for each sample collect a <code>20</code>
minutes extent worth of data.

```json
{
  "unit": "minutes",
  "size": 10,
  "extent": 20
}
```

### QueryAggregation

A single aggregation step.

A QueryAggregation has a __type__ field defining its type.

The following types are available.

+ Average - Calculate an average over all datapoints in each sampling period.
+ Sum - Calculate a sum over all datapoints in each sampling period.

###### Structure

See also:
+ [QuerySampling](#querysampling)

```yml
Average:
  type: required "average"
  # The sampling period and extent to use.
  sampling: required QuerySampling

Sum:
  type: required "sum"
  # The sampling period and extent to use.
  sampling: required QuerySampling
```

### MetricsRequest

A request for metrics.

###### Structure

```yml
MetricsRequest:
  # The time range for which to query.
  range: required QueryDateRange
  # The key to query for
  key: required String
  # A statement used to filter down the selected time series.
  filter: optional [Filter](#filter)
  # A list of tags which will be used to group the result.
  groupBy: optional [String, ..]
  # The chain of aggregators to use for this request.
  aggregators: optional [QueryAggregation, ..]
  # Match the exact set of tags specified (will deprecated in favor of only "filter").
  tags: optional {String: String, ..}
  # Match that the specified tag names are present (will deprecated in favor of only "filter").
  hasTags: optional [String, ..]
```

###### Example

```json
{
  "range": {"type": "relative"},
  "key": "foo",
  "filter": ["key", "foo"],
  "groupBy": ["site"],
  "aggregators": [],
  "tags": {"foo": "bar"},
  "hasTags": ["role"]
}
```

### MetricsResponse

The result of a query.

###### Structure
```yml
MetricsResponse:
  # The range which this result contains.
  range: required DateRange
  # An array of results.
  result: required [ResultGroup, ..]
  # Statistics about the current query.
  # This field should be inspected for errors which will have caused the result
  # to be inconsistent.
  statistics: required Statistics
  
ResultGroup:
  # An unique hash for this specific time series.
  hash: required String
  # The key of the time series.
  key: required String
  # The tags of the time series group (only for groupBy).
  # If groupBy for the request is empty, this will be an empty object.
  tags: required {String: String, ..}
  # An array of data points.
  values: required [DataPoint, ..]
```

###### Example
```json
{
  "result": [
    {
      "hash": "deadbeef",
      "tags": {"foo": "bar"},
      "values": [[1300000000000, 42.0]]
    },
    {
      "hash": "beefdead",
      "tags": {"foo": "baz"},
      "values": [[1300000000000, 42.0]]
    }
  ],
  "range": ...,
  "statistics": ...
}
```
