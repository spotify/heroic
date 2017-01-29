# How to Align Timestamps

Strongly define how time buckets are aligned, specifically with an half-open interval
`[start, end)`.

* **Status** Draft
* **Author** John-John Tedro <udoprog@spotify.com>

## Suggestion

The current time buckets have values at timestamp `t` include the range `(t - extent, t]` in each
bucket.

Define a `DateRange` to be `[start, end)`. And buckets at timestamp `t`, should include data in the
interval `[t, t + extent)`.

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

* OpenTSDB, used to have a timestamp which is the average of all data in a given bucket, but now
  aligns it to the beginning of the interval (as proposed here)
* InfluxDB, is slightly inconsistent, but most aggregation functions appear to have a timestamp
  aligned to the beginning.
* Graphite, aligned at beginning of period.
