# Querying for Metrics

This details the API endpoints that are used for querying for data out of Heroic.

## POST /metrics

The simplest query method, expects a [MetricsRequest](#metricsrequest), and will return a [MetricsResponse](#metricsresponse) object when successful.

+ Response 200 (application/json) [MetricsResponse](#metricsresponse)
+ Response 500 (application/json) [ErrorMessage](#errormessage)

# Types

### DateRangeRequest

Defines a DateRange, but with a much more flexible format.

Has the following types.

+ AbsoluteDateRangeRequest - Queries an absolute time range.
+ RelativeDateRangeRequest - Queries a time range which is relative to the current time.

###### Structure (AbsoluteDateRangeRequest)
```javascript
{
  "type": "absolute",
  /**
   * Starting timestamp in milliseconds from the unix epoch.
   */
  "start": <number>,
  /**
   * Ending timestamp in milliseconds from the unix epoch.
   */
  "end": <number>,
}
```

###### Structure (RelativeDateRangeRequest)
```javascript
{
  "type": "absolute",
  /**
   * Unit to use for 'value'.
   */
  "unit": <"MILLISECONDS"|"SECONDS"|"MINUTES"|"HOURS"|"DAYS"|"WEEKS"|"MONTHS">
  /**
   * How many 'unit' timespans back in time this date starts.
   */
  "value": <number>,
}
```

###### Example

```javascript
{"start": 1300000000000, "end": 1400000000000}
```

### DateRange

A date range, starting at __start__ milliseconds from the unich epoch and ending at __end__ milliseconds from the unix epoch.

###### Structure
```javascript
{
  /**
   * Starting timestamp in milliseconds from the unix epoch.
   */
  "start": <number>,
  /**
   * Ending timestamp in milliseconds from the unix epoch.
   */
  "end": <number>,
}
```

###### Example

```javascript
{"start": 1300000000000, "end": 1400000000000}
```

### DataPoint

A datapoint is represented as an array with two elements.
The __first__ element is the timestamp which is milliseconds from the unix epoch.
The __second__ element is the value.

###### Structure
```javascript
[<number>, <number>]
```

###### Example

```javascript
[1300000000000, 42.0]
```

### Statistics

###### Structure
```javascript
{
  "aggregator": {
    /**
     * How many original datapoints were involved to perform the required
     * aggregates.
     */
    "sampleSize": <number>,
    /**
     * How many original datapoints were out of bounds for the specified
     * aggregate.
     * This indicates that heroic has queried unecessary data from the backends.
     */
    "outOfBounds": <number>,
  },
  "row": {
    /**
     * How many successful database row was loaded into heroic.
     */
    "successful": <number>,
    /**
     * How many database rows failed to be loaded by heroic.
     */
    "failed": <number>,
    /**
     * How many row fetches were cancelled.
     */
    "cancelled": <number>,
  },
  "cache": {
    /**
     * How many resulting data points could be fetched from cache.
     */
    "hits": <number>,
    /**
     * How many cached data points conflicted with each other.
     */
    "conflicts": <number>,
    /**
     * How many calculated data points conflicted with the ones from cache.
     */
    "cacheConflicts": <number>,
    /**
     * How many cached NaN's that were loaded.
     */
    "cachedNans": <number>,
  },
  "rpc": {
    /**
     * How many successful RPC requests were executed for this query.
     */
    "successful": <number>,
    /**
     * How many failed RPC requests.
     */
    "failed": <number>,
    /**
     * How many cluster nodes were considered online during the query.
     */
    "onlineNodes": <number>,
    /**
     * How many cluster nodes were considered offline during the query.
     */
    "offlineNodes": <number>,
  },
}
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

### FilterStatement

###### Structure

```javascript
[<"and"|"or"|"="|"+"|"key">, ..]
```

### MetricsRequest

A request for metrics.

###### Structure

```javascript
{
  /**
   * A statement used to filter down the selected time series.
   */
  "filter": <FilterStatement>,
  /**
   * A list of tags which will be used to group the result.
   */
  "groupBy": [<string>, ..],
  /**
   * The time range for which to query.
   */
  "range": <DateRangeRequest>,
  /**
   * The chain of aggregators to use for this request.
   */
  "aggregators": [<AggregatorRequest>, ..]
}
```

### MetricsResponse

The result of a query.

###### Structure
```javascript
{
  /**
   * The date range that was queries.
   */
  "range": <DateRange>,
  /**
   * An array of results.
   */
  "result": [{
    /* An unique hash for this specific time series. */
    "hash": <string>,
    /* The key of the time series. */
    "key": <string>,
    /* The tags of the time series. */
    "tags": {<string>: <string>, ...},
    /* An array of datapoints. */
    "values": [<DataPoint>, ...],
  }, ...],
  /**
   * Statistics about the current query.
   * This field should be inspected for errors which will have caused the result
   * to be inconsistent.
   */
  "statistics": <Statistics>,
```

###### Example
```
TODO: Make example
```
