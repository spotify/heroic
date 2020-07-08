---
title: BigTable
---
# BigTable 

Some good to know points about how Heroic uses Bigtable

## Row Key composition/schema

Heroic stores time series in BigTable in a schema optimized for retrieving data for the purpose of dashboarding, for example retrieving and aggregating the CPU load on all machines of a backend service in a particular region. This is achieved by storing tags as part of the row keys in BigTable. 

Heroic’s BigTable schema stores all metrics with the same key, tags, and resource identifiers within roughly 50 days (4294967296 milliseconds) in the same row. All metrics within those 50 days are stored as separate cells of data, with the time period within the 50 days as column key (or column qualifier), and the measurement as data.

This way, Heroic avoids having a single row for *all* data points in a time series from the beginning of time until the heat death of the universe.


Given the two datapoints for an example timeseries:

```json
{
 "series": {
     "key": "system",
     "tags": {
       "site": "gew",
       "what": "cpu-idle-percentage",
       "system-component": "cpu",
       "cpu-type": "idle",
       "unit": "%"
     },
     "resource": {
       "podname": "pod-example-123-abc",
       "host": "database.example.com"
     }
 },
 "data": {
   "type": "points",
   "data": [[1300000000000, 42.0]]
 }
}
```

and

```json
{
 "series": {
     "key": "system",
     "tags": {
       "site": "gew",
       "what": "cpu-idle-percentage",
       "system-component": "cpu",
       "cpu-type": "idle",
       "unit": "%"
     },
     "resource": {
       "podname": "pod-example-123-abc",
       "host": "database.example.com"
     }
 },
 "data": {
   "type": "points",
   "data": [[1300001000000, 84.0]]
 }
}

```

In order to retain the exact timestamp, Heroic splits the original timestamp into the base-timestamp (to be used for the row key), and the delta, or offset timestamp (to be used in the column key, or qualifier). When queried, the sum of the base-timestamp in the row key and the delta timestamp in the column qualifier becomes the exact timestamp.

To illustrate this, using the two metrics in the example above, Heroic calculates the base-timestamp and the delta timestamp as follows:



Base-timestamp first metric: <span style="background-color:powderblue; color:black">1300000000000</span> - (<span style="background-color:powderblue; color:black">1300000000000</span>  % <span style="background-color:lightcyan; color:black">4294967296</span> ) = <span style="background-color:blue; color:black">1297080123392</span>
Timestamp delta first metric: <span style="background-color:powderblue; color:black">1300000000000</span> - <span style="background-color:blue; color:black">1297080123392</span> = <span style="background-color:grey; color:black">2919876608</span>
 
Base-timestamp second metric: <span style="background-color:powderblue; color:black">1300001000000</span> - ( <span style="background-color:powderblue; color:black">1300001000000</span> % <span style="background-color:lightcyan; color:black">4294967296</span> ) = <span style="background-color:blue; color:black">1297080123392</span>
Timestamp delta second metric: <span style="background-color:powderblue; color:black">1300001000000</span> - <span style="background-color:blue; color:black">1297080123392</span> = <span style="background-color:grey; color:black">2920876608</span>



Together with all tags and resources, sorted lexicographically, this ends up creating this row-key: 

<span style="background-color:red; color:black">system</span>,<span style="background-color:yellow; color:black">cpu-type=idle,site=gew,system-component=cpu,unit=%,what=cpu-idle-percentage</span>,<span style="background-color:blue; color:black">1297080123392</span>,<span style="background-color:green; color:black">database.example.com,pod-example-123-abc</span>


And the two metrics sent 16 minutes apart are stored in the BigTable table like so:


| Row key                                                                                                                                   	| <span style="background-color:grey; color:black">2919876608</span> 	| <span style="background-color:grey; color:black">2920876608</span> 	|
|-------------------------------------------------------------------------------------------------------------------------------------------	|------------	|------------	|
| <span style="background-color:red; color:black">system</span>,<span style="background-color:yellow; color:black">cpu-type=idle,site=gew,system-component=cpu,unit=%,what=cpu-idle-percentage</span>,<span style="background-color:blue; color:black">1297080123392</span>,<span style="background-color:green; color:black">database.example.com,pod-example-123-abc</span> 	| <span style="background-color:pink; color:black">42.0</span>       	| <span style="background-color:pink; color:black">84.0</span>       	|


***