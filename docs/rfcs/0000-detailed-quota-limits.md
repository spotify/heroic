# Detailed Quota Limtis

Quota limits are currently communicated through the `.limits` field in the MetricsResponse.

This only includes information about what kind of quota was violated, while a user would mostly
be interested in what the effective limits violated are so that they can be worked around.

* **Status** Draft

# Suggestion

This suggestion is to change `ResultLimit` into an interface, which has different subtypes depending on the type of limit that was violated.

The `.limits` field would then change from:
```json
{"limits": ["SERIES"]}
```

Into:

```json
{
  "limits": [{
    "type": "series",
    "seriesLimit": 20000
  }]
}
```

This enhancement would allow the result set to communicate what the limit was, at the time of the query. This information can then be presented to the user, like:

```
Query required too many time series to be processed. The current limit is: 20000.
```
