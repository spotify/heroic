# Detailed query logging


We want to provide a configurable feature that gives a detailed log of how the queries are treated
by Heroic.

* **Status** Implemented
* **Author** John-John Tedro <udoprog@spotify.com>, Gabriel Gerhardsson <gabrielg@spotify.com>,
             Martin Parm <parmus@spotify.com>   
* **PR** https://github.com/spotify/heroic/pull/199


## Suggestion

The user can provide the system with additional `clientContext` which will be included in each log
step. The system will generate a `queryId`, when the query has been received , which can be used to
group or correlate individual queries.

Each log line has the following structure:

```json
{
  "component": "com.spotify.heroic.query.CoreQueryManager",
  "queryId": "ed6fe51c-afba-4320-a859-a88795c15175",
  "clientContext": {
    "dashboardId": "my-system-metrics",
    "user": "udoprog"
  },
  "type": "Received",
  "data": {}
}
```

`data` would be specific to the type being logged.

In all relevant stages of query processing, a call to the query logging framework is done, which
results in logging of json describing the exact structure of the query in that stage. 

Suggested relevant stages and data to log:

* **api** `Query` as received from the user, also includes relevant HTTP-based information.
* **api** `Query` as received by ClusterManager.
* **api** `FullQuery.Request` which fans out to all data nodes.
* **data** `FullQuery.Request` when received by data node.
* **data** `FullQuery` as it goes back to the API node (excluding data).
* **api** `FullQuery` as received from data node (excluding data).
* **api** `QueryMetricsResponse` being sent to the user (excluding data).

*Note*: All response paths excludes samples (time series data) to reduce the size of the log. This
would be represented as the size of the data being returned instead.

#### Requirements

* Query logging framework is isolated from critical path of query processing in a way so that
failure in the query logger doesn't affect query processing.

#### Extension to Query API

The following field would be added to the query API.

```
{
  "features": ["com.spotify.heroic.query_logging"],
  "clientContext": {
    "dashboardId": "my-system-metrics",
    "user": "udoprog"
  }
}
```

This is completely optional, and query logging can be enabled globally by setting the feature flag
`com.spotify.heroic.query_logging`.

#### Configuration

Query logging is configured in the configuration file under the `queryLogging` section, like the
following:

```yaml
queryLogging:
  type: file
  path: /path/to/query.log
  rotationPeriod: 1d
```

Or with a logger facility:

```yaml
queryLogging:
  type: slf4j
  name: com.spotify.heroic.query_logging
```
