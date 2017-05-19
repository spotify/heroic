# How to Align Timestamps

Strongly define how time buckets are aligned, specifically with an half-open interval
`[start, end)`.

* **Status** Implemented
* **Author** John-John Tedro <udoprog@spotify.com>
* **PR** https://github.com/spotify/heroic/pull/208

## Requirements

The output buckets of sampling aggregations must fall within the captured interval of the same bucket in the next sampling aggregation.
This avoids samples 'travelling' when chaining multiple sampling aggregations, so that `a | b` is equivalent to `a | a | b`. A concrete example is that `spread | sum` must result in the same buckets as just `sum` to allow for correctly functioning global aggregations.

Current fetches for `BigtableBackend` have one edge case for every period interval where data is invisible (see: [AbstractMetricBackendIT.java](https://github.com/spotify/heroic/blob/master/heroic-test-it/src/main/java/com/spotify/heroic/test/AbstractMetricBackendIT.java#L193)).
An interval definition should be picked which makes this easier to avoid.

## Suggestion

The current time buckets have values at timestamp `t` include the range `(t - extent, t]` in each
bucket.

Define a date range (see current `DateRange`) to be `[start, end)`. Buckets at timestamp `t`, should include data in the
interval `[t, t + extent)`.

This has partly been implemented in https://github.com/jo-ri/heroic/pull/1 - but should be built in a way that it can be applied prior to https://github.com/spotify/heroic/pull/206 so that test coverage can be increased before the change.

#### Example

Assuming data:
```
00:00 1.0
00:01 2.0
00:02 3.0
00:03 4.0
00:04 5.0
00:05 6.0
00:06 7.0
00:07 8.0
```

With the old definition of an interval, `sum(size=2m)` would result in:

```
00:00: 1.0
00:02: 5.0
00:04: 9.0
00:06: 13.0
00:08: 8.0
```

And with the suggested change, `sum(size=2m)` would be:

```
00:00: 3.0
00:02: 7.0
00:04: 11.0
00:06: 15.0
```

#### Other TSDBs

* [OpenTSDB](http://opentsdb.net/docs/build/html/user_guide/query/downsampling.html),
  used to have a timestamp which is the average of all data in a given bucket, but now
  aligns it to the beginning of the interval (as proposed here).
* InfluxDB, is slightly inconsistent, but most aggregation functions appear to have a timestamp
  aligned to the beginning.
* Graphite, aligned at beginning of period.
