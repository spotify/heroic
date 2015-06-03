The following sections document API endpoints for heroic.

# Querying for Metrics

This details the API endpoints that are used for querying for data out of Heroic.

### GET /status

Query for the status of a heroic instance.

The status code <code>503</code> is used to indicate to load balancers that a
service is not available for requests right now.

+ Response ```200``` (application/json) *See below*
+ Response ```503``` (application/json) *See below*
 + Used to indicate to load-balancers that this node is unsuitable to receive requests.
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)

### POST /query/metrics

Endpoint used for querying and aggregating metrics.

+ Request (application/json) *See below*
+ Response ```200``` (application/json) *See below*
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)
+ **deprecated** Alias `/metrics`

###### Request Structure

* `range` **required** [`QueryDateRange`](#querydaterange)
  * The range in time for which to query.
* `filter` **optional** [`Filter`](#filter)
  * A statement used to filter down the selected time series.<br/>
    Each individual filtering field (`filter`, `key`, `tags`, and `hasTags`) may be empty.
    However at least one must be specified to make up a valid filter.
* `aggregators` **optional** [`[QueryAggregation, ..]`](#queryaggregation)
  * Aggregators used to downsample the response.
* `groupBy` **optional** `[String, ..]`
  * Will create several results group with the given keys.<br/>
    Each unique tag value for the above key will result in its own result group.<br/>
    * Given time series `{"a": 1, "b": 2}` and `{"b": 3, "c": 4}`, and a `group` of `["a", "b"]`
      the returned result groups would be `[{"a": 1, "b": 2}, {"a": null, "b": 3}]`.<br/>
      Time series are aggregated on a per-group basis.
* `key` **optional** `String`
  * The key to query for.<br/>
    **deprecated**: Use `filter` with a `["key", <key>]` statement instead.
* `tags` **optional** `{String: String}`
  * Match the exact set of tags specified.<br/>
    **deprecated**: Use `filter` with several `["=", <key>, <value>]` statements instead.
* `hasTags` **optional** `[String]`
  * Match if the specified tag keys are present.<br/>
    **deprecated**: Use `filter` with several `["+", <key>]` statements instead.

###### Request Example

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

###### Response Structure

* `ok` `boolean`
  * Indicates if this node is ok or not.
* `consumers`
  * `ok` `boolean`
    * Indicates if all the consumer instances are 'ok'.
  * `available`
    * How many consumer instances are available.
  * `ready`
    * How many consumer instances are ready.
  * `errors`
    * How many consumer errors have been encountered (in total).
* `backends`
  * `ok` `boolean`
    * Indicates if all backend instances are 'ok'.
  * `available`
    * How many backend instances are available.
  * `ready`
    * How many backend instances are ready.
* `metadataBackends`
  * `ok` `boolean`
    * Indicates if all metadata backend instances are 'ok'.
  * `available`
    * How many metadata backend instances are available.
  * `ready`
    * How many metadata backend instances are ready.
* `cluster`
  * `ok` `boolean`
    * Indicates if the cluster is 'ok'.
  * `onlineNodes`
    * How many nodes that are considered 'online' and available.
  * `offlineNodes`
    * How many nodes that are considered 'offline' and unavailable.

###### Response

* `range` [`QueryDateRange`](#querydaterange)
  * The range in time for which to query.
* `errors` [`[RequestError, ..]`](#requesterror)
  * Potential errors returned either from different shards or for specific time series.
    The presence of an error does not cause the entire query to fail, instead it is up
    to the client to use this information to decide if the response is *reliable enough*.
* `result` [`[MetricGroup, ..]`](#metricgroup)
  * An array of result groups.
* `statistics` [`MetricsQueryStatistics`](#metricsquerystatistics)
  * Statistics about the current query.
  * This field should be inspected for errors which will have caused the result
  * to be inconsistent.

###### RequestError

* `type` `"node"`
* `nodeId` `String`
  * Id of the node that failed.
* `nodeUri` `String`
  * URI of the node that failed.
* ``tags` `{String: String}`
  * Shard tags of the node that failed.
* `error` `String`
  * Error message.
* `internal` `boolean`
  * Boolean indicating weither the node failure was due to an internal (local) or external (remote) error.
  
* `type` `"series"`
* ``tags` `{String: String}`
  * Shard tags of the node that failed.
* `error` `String`
  * Error message.
* `internal` `boolean`
  * Boolean indicating weither the node failure was due to an internal (local) or external (remote) error.

###### MetricGroup

* `hash` `String`
  * A hash for this specific result group.
* ``tags` `{String: String}`
  * `null` if `groupBy` is not specified in the request<br/>
    *otherwise* The tags of the time series group.
* ``shard` `{String: String}`
  * The shard that this metric group came from.<br/>
    Multiple shards can potentially return the same groups (global aggregates).
* `values` [`[DataPoint, ..]`](#datapoint)
  * An array of data points that make up the time series.
* `key` `null`
  * **deprecated** Always null, used to be the key of the time series.

###### Response Example

```json
{
  "errors": [
    {
      "type": "node",
      "nodeId": "abcd-efgh",
      "nodeUri": "http://example.com",
      "tags": {"site": "lon"},
      "error": "Connection refused",
      "internal": true
    },
    {
      "type": "series",
      "tags": {"site": "lon"},
      "error": "Aggregation too heavy, too many rows from the database would have to be fetched to satisfy the request!",
      "internal": true
    }
  ],
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
  "range": {},
  "statistics": {}
}
```

### POST /write/metrics

Used for writing data into heroic directly.

+ Request (application/json) *See below*
+ Response ```200``` (application/json) *See below*
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)

###### Request Structure

* `series` **required** [`Series`](#series)
  * Time series to write data to.
* `data:` **required** [`[DataPoint, ..]`](#datapoint)
  * Data to write.

###### Request Example

```json
{"series": {"key": "foo", "tags": {"site": "lon", "host": "www.example.com"}},
 "data": [[1300000000000, 42.0], [1300001000000, 84.0]]}
```

###### Response Structure

* `ok` `boolean`
  * Indicates weither write was successful or not.

###### Response Example

```json
{"ok": true}
```

# Metadata

### POST /metadata/tags

Use to query for tags.

+ Request (application/json) [MetadataQueryBody](#metadataquerybody)
+ Response ```200``` (application/json) [MetadataQueryBody](#metadataquerybody)
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)
+ **deprecated**: Alias `/tags`

###### Response Structure

* `result` `{String: [String, ..], ..}`
  * Contains a set of available tags and all their corresponding permutations as an array `[String, ..]`.
* `sampleSize` `Number`
  * The number of time series that had to be scanned over to build this response.

###### Response Example

```json
{"result": {"role": ["database", "webfrontend"],
            "host": ["database.example.com", "webfrontend.example.com"]},
 "sampleSize": 2}
```

### POST /metadata/keys

Use to query for tags.

+ Request (application/json) [MetadataQueryBody](#metadataquerybody)
+ Response ```200``` (application/json) *See below*
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)
+ **deprecated**: Alias `/keys`

###### Response Structure

* `result` `[String, ..]`
* `sampleSize` `Number`
  * The number of samples that was scanned.

### GET /metadata/series

Use to query for series.

This is the way to return the exact combination of series that are available.

+ Request (application/json) [MetadataQueryBody](#metadataquerybody)
+ Response ```200``` (application/json) *See below*
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)

###### Response Structure

* `result` `[Series, ..]`
 * A list of time series making up the response>
* `sampleSize` `Number`
 * The number of samples that had to be scanned to build this response.

### GET /metadata/tagkey-suggest

Use to query only for tag keys.

This is a way to get a list of available key combinations under a given filter.

+ Request (application/json) *See below*
+ Response ```200``` (application/json) *See below*
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)

###### Request Structure

* `filter` **required** [`Filter`](#filter)
  * Apply the given filter to the search.
* `key` **required** `String`
  * Match the given key, does not have to be complete.
* `limit` **optional** `Number`
  * Limit the number of results that you want to see.<br/>
    **default**: `10`

###### Request Example

```json
{
  "filter": ["key", "system"],
  "key": "rol",
  "limit": 10
}
```

###### Response Structure

* `result` `[{"score": Number, "key": String}, ..]`
  * Results for the given completion, ordered by score.<br/>
    `score` indicates how good the result matches.
    A score equal or above `1.0` is considered a very good match.<br/>
    `key` is the exact string for the tag key that should be completed.
* `sampleSize` `Number`
  * The number of samples that had to be scanned to build this response.

### GET /metadata/tagvalue-suggest

Use to query only for tag keys.

This is a way to get a list of available key combinations under a given filter.

+ Request (application/json) *See below*
+ Response ```200``` (application/json) *See below*
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)

###### Request Structure

* `filter` **required** [`Filter`](#filter)
  * Apply the given filter to the search.
* `key` **required** `String`
  * Must be an exact matching key.
* `value` **required** `String|null`
  * Match the given value.<br/>
    It does not have to be an exact match.<br/>
    `null` indicates that this field should be ignored.
* `limit` **optional** `Number`
  * Limit the number of results that you want to see.<br/>
    **default**: `10`

###### Request Example

```json
{
  "filter": ["key", "system"],
  "key": "role",
  "value": "databa",
  "limit": 10
}
```

###### Response Structure

* `result` `[{"score": Number, "value": String}, ..]`
  * Results for the given completion, ordered by score.<br/>
    `score` indicates how good the result matches.
    A score equal or above `1.0` is considered a very good match.<br/>
    `value` is the exact value for for a given tag.
* `sampleSize` `Number`
  * The number of samples that had to be scanned to build this response.

###### Response Example

```json
[
  {"score": 10.0, "value": "database"},
  {"score": 0.1, "value": "frontend"}
]
```

### GET /metadata/tag-suggest

Use to query for tag pair completions.

This is the way to return the exact combination of a specific tag that is available under a given filter.

+ Request (application/json) *See below*
+ Response ```200``` (application/json) *See below*
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)

###### Request Structure

* `filter` **required** [`Filter`](#filter)
  * Apply the given filter to the search.
* `key` **required** `String|null`
  * Search for the given key.<br/>
    Does not have to be an exact match.<br/>
    `null` indicates that this field should be ignored.
* `value` **required** `String|null`
  * Match the given value.<br/>
    Does not have to be an exact match.<br/>
    `null` indicates that this field should be ignored.
* `limit` **optional** `Number`
  * Limit the number of results that you want to see.<br/>
    **default**: `10`

###### Request Example

```json
{
  "filter": ["key", "system"],
  "key": "role",
  "value": "datab",
  "limit": 10
}
```

###### Response Structure

* `result` `[{"score": Number, "key": String, "value": String}, ..]`
  * Results for the given completion, ordered by score.<br/>
    `score` indicates how good the result matches.
    A score equal or above `1.0` is considered a very good match.<br/>
    `key` is the exact key for a tag matching the query.<br/>
    `value` is the exact value for a tag matching the query.<br/>
* `sampleSize` `Number`
  * The number of samples that had to be scanned to build this response.

###### Response Example

```json
[
  {"score": 10.0, "key": "role", "value": "database"},
  {"score": 0.1, "key": "role", "value": "frontend"}
]
```

# RPC Endpoints

These endpoints are used internally by the cluster.

All other RPC endpoints except ```/rpc/metadata``` are versioned.
The versioned endpoints differ, but can all be found under ```/rpc<version>``` where ```version``` is a nodes provided version.

### GET /rpc/metadata

Query a node for its metadata.

The metadata is used to determine which _shard_ a node belongs to through its ```tags``` attributes.
It is also used to determine which ```capabilities``` a node has which changes which type of requests it can receive.
Finally it provides a ```version``` which allows other node to determine how this node should be communicated with, and which features it support.

+ Response ```200``` (application/json) *See below*
+ Response ```4xx``` or ```5xx``` (application/json) [ErrorMessage](#errormessage)

###### Request Structure

* `version` `Number`
* `id` `String`
* `tags` `{String: String}`
* `capabilities` `["QUERY"|"WRITE", ..]`

The following are the available capabilities and their corresponding meaning.

+ ```QUERY``` - Means that the node can receive queries.
+ ```WRITE``` - Means that the node can receive writes.

###### Request Example

```json
{"version": 2, "id": "431f3900-33be-11e4-8c21-0800200c9a66", "tags": {"site": "lon"}, "capabilities": ["QUERY"]}
```

# Common Types

The following are structure documentation for common (shared) types in this documentation.

### ErrorMessage

Contains a human readable error that can be returned by the API.

Is typically returned by ```4xx``` and ```5xx``` HTTP status code family responses.

###### Structure

* `message` `String`
 * A message describing the error that occured.

###### Example
```json
{"message": "Something gone doofed!"}
```

# Metadata Types

### MetadataQueryBody

###### Request Structure

* `filter` **optional** `Filter`
 * A general set of filters. Gives the most amount of control using the filtering types.<br/>
   If this is combined with `matchKey`, `matchTags`, or `hasTags`, the generated filter
   will be an `and` statement combining all of the specified criteria.
* `matchKey` **optional** `String`
 * Only include time series which match the exact key.<br/>
   **deprecated**: Use `filter` instead with a `["key", String]` statement.
* `matchTags` **optional** `{String: String}`
  * Only include time series which matches the exact key/value combination.<br/>
    **deprecated**: Use `filter` instead with multiple `["=", String, String]` statements.
* `hasTags:` **optional** `[String, ..]`
  * Only include time series which has the following tags.<br/>
    **deprecated**: Use `filter` instead with multiple `["+", String]` statements.

###### Request Example

The following examples are all equivalent, however only the first one is recommended.

```json
{"filter": ["and", ["key", "system"], ["=", "role", "database"]]}
{"filter": ["=", "role", "database"], "matchKey": "system"}
{"filter": ["key", "system"], "matchTags": {"role": "database"}}
```

# Types

The following are structure documentation for all available types in the API.

### Series

The identity of a time series.

###### Structure

* `key` `String`
 * The key of the time series.
* `tags` `{String: String}`
 * The tags of the time series.

###### Example

```json
{"key": "foo", "tags": {"site": "lon", "host": "www.example.com"}}
```

### QueryDateRange

Defines a DateRange, but with a much more flexible format.

A QueryDateRange has a __type__ field defining its type.

The following types are available.

+ Absolute - Queries an absolute time range.
+ Relative - Queries a time range which is relative to the current time.

###### Structure

* `type` `"absolute"`
 * Indicating that this is an absolute value date range.
* `start` `Number`
 * Starting timestamp in milliseconds from the unix epoch.
* `end` `Number`
 * Ending timestamp in milliseconds from the unix epoch.

* `type` `"relative"`
 * Indicating that this is an relative value date range.
* `unit` `"MILLISECONDS"|"SECONDS"|"MINUTES"|"HOURS"|"DAYS"|"WEEKS"|"MONTHS"`
 * Unit to use for `value`.
* `value` `Number`
 * How many `unit` timespans back in time this date starts.

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

* `start` `Number`
  * Starting timestamp in milliseconds from the unix epoch.
* `end` `Number`
  * Ending timestamp in milliseconds from the unix epoch.

###### Example

```json
{"start": 1300000000000, "end": 1400000000000}
```

### DataPoint

A datapoint is represented as an array with two elements.
The __first__ element is the timestamp which is milliseconds from the unix epoch.
The __second__ element is the value.

###### Structure

`[Number, Number]`

The first number is the timestamp in milliseconds since unix epoch.

The second number is the value at the given timestamp.

###### Example

```json
[1300000000000, 42.0]
```

### MetricsQueryStatistics

Comprehensive statistics about the executed query.

This object contains tons of valuable information that should be analyzed on each request to get an idea of how __valid__ the result is.

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

###### Structure

* `aggregator`
  * `sampleSize`
    * How many original data points were involved to perform the required
      aggregates.
  * `outOfBounds`
    * How many original datapoints were out of bounds for the specified
      aggregate.
* `row`
  * `successful`
    * How many rows were successfully fetched from the backend.
  * `failed`
    * How many rows failed to be fetched from the backend.
  * `cancelled`
    * How many rows were not fetch because of cancellation.
* `cache`
  * `hits`
    * How many cached data points conflicted with each other.
  * `conflicts`
    * How many cached data points conflicted with each other.
  * `cacheConflicts`
    * How many calculated data points conflicted with the ones from cache.
  * `cachedNans`
    * How many cached NaN's (invalid) were loaded.
* `rpc`
  * `successful`
    * How many successful RPC requests were executed for this query.
  * `failed`
    * How many failed RPC requests.
  * `onlineNodes`
    * How many cluster nodes were considered online during the query.
  * `offlineNodes`
    * How many cluster nodes were considered offline during the query.

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



* `["and", Filter, ..]`
  * Match if all child filter statements match.
* `["or", Filter, ..]`
  * Match if any child filter statements match.
* `["not", Filter]`
  * Match if child filter statement does not match. 
* `["key", String]`
  * Match if a time series has the specified key.
* `["=", String, String]`
  * Match if a time series has the specified tag and value combination.
* `["+", String]`
  * Match if a time series has the specified tag.
* `["^", String, String]`
  * Match if a tag starts with the specified value.
* `["~", String, String]`
  * Match if a tag matches a specific regular expression.

###### Example

The following example will select time series that the key is either `foo` or `bar`, the `host` tag is set to `foo.example.com`, the `role` tag is set to `example`, a tag called `site` is present, and that the `site` tag is _not_ equal to `lon`. 

```json
["and",
  ["or", ["key", "foo"], ["key", "bar"]],
  ["=", "host", "foo.example.com"],
  ["=", "role", "example"],
  ["+", "site"],
  ["not", ["=", "site", "lon"]]
]
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

* `type` **required** `"average"`
* `sampling (require)` [`QuerySampling`](#querysampling)
  * The sampling period and extent to use.
  
* `type` **required** `"sum"`
* `sampling (require)` [`QuerySampling`](#querysampling)
  * The sampling period and extent to use.

###### QuerySampling

* `unit` **required** `"MILLISECONDS"|"SECONDS"|"MINUTES"|"HOURS"|"DAYS"|"WEEKS"|"MONTHS"`
  * The time unit to use for the `size` field.
* `size` **required** `Number`
  * The size of the sampling period to use in accordance with the given `unit`.
* `extent` **optional** `Number`
  * The extent for which the request should get data for each sampling period
    in accordance with the given time `unit`.<br/>
    Defaults to the same as `size`.
