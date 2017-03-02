# Make sampling a first class citizen _(working title)_


We want to make sampling a first class citizen rather than a side-effect of the first aggregation.
This will address a number of problems:

* Users are often confused about the raw unsampled data, and forgetting to add a down-sampling
  aggregation might lead to invalid results and conclusions.


* **Status** Draft
* **Author** Martin Parm <parmus@spotify.com>   
* **PR** https://github.com/spotify/heroic/pull/225


## Suggestion

User can provide an optional sampling configuration with their query.


```json
{
  "resolution": {
    "unit": "MINUTES",
    "value": 5
  },
  "samplingMethod": "average"
}
```

Heroic will pick a default sampling, if no sampling configuration is provided.
The query result will contain a similar structure to inform the user about, which sampling
methods and resolution was used in the query.

### Sampling methods

The following sampling methods should be supported:

- `average`
- `max`
- `min`
- `first`
- `last`

### Resolution
_(To be written)_

### Predictable on memory consumption

Sampling can be applied while loading data points from the backend and the resulting number of data
points will be upper-bound by _numberOfTimeseries_ * (_timeRange_ / _resolution_), thus making
the memory consumption very predictable.
This upper bound can even by calculated using just the metadata, making it possible to reject
heavy queries without loading any data from the backend.

The same predictability also makes it possible to implement an auto-resolution feature with a similar
predictable memory comsumption. However this is will be addressed in a different RFC.


## Extensions

This section contains optional extensions to this RFC, which could provide further benefits.

### New endpoint for fetching raw data points
 
Users can currently fetch the raw un-sampled and un-aggregated data points by simply not
specifying an aggregation. With this RFC sampling will always be mandatory; either explicitly
specified by the user or implicitly applied by Heroic. To allow users to still access the raw
data point a endpoint is proposed. This endpoint should support the same filtering parameters
as the query endpoint, but neither sampling nor aggregation should be supported.

### Upsampling (filling in missing datapoints)

In addition to downsampling datapoints to a lower resolution, it would also be able to
upsample to a higher resolution; or "fill in" missing data points. The following methods are
proposed:

- `null` - Simply fill in with nulls, which is the same as no upsampling. This already
 happens today.
- `repeatLast` - Repeat the last data point
- `zero` - Set missing data points to zero

The upsampling method should be specified separately from the downsampling method as it's
not possible to predict the resolution of the original data, and thus not possible to predict
whether the data points should be upsampled or downsampled.  

```json
{
  "resolution": {
    "unit": "MINUTES",
    "value": 5
  },
  "samplingMethod": "average",
  "missing": "null"
}
```

Notice that the configuration parameter is called `missing` to make it explicit To make it
explicitly clear that upsampling deals with missing data points.


### Simplifying the aggregation engine

The aggregation engine currently have to support an unknown number of data points per bucket in
every step due to the unpredictable nature of the initial unsampled data.
With the more predictable natue of sampled data, the aggregation engine can assume that each time
series always have exactly 0 or 1 data point per bucket.

The aggregation engine also currently deals with resolution at every aggregation step, however we
have observed very little practical use of this feature. As sampling will effective deal with
resolution before the data points reach the aggregation engine, we can remove this complication
from the aggregation engine completely.

These two changes should result in a much simpler aggregation engine with more predictable run-time
behavior and memory consumption.  
