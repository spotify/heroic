---
title: Getting Started
---
# {{ page.title }}

After reading the <a href="docs/overview">Heroic Overview</a> and getting a feel for Heroic by running the [docker image][2] it's time to talk about the production setup. 

To recap, Heroic consists of two components:

1. API node
1. Consumer node
 
The API node is responsible for getting metrics out. It receives user traffic over HTTP, fetches metrics from the backend, and aggregations the results.

The consumer node is responsible for getting metrics in. It uses a message queue to receive metrics produced by [ffwd][1] or a similar system and writes the metrics to a durable backend. For each metric there are 3 different writes made for full API functionality. A consumer can perform all the write functions or be split up.

1. Metric backend
1. Metadata backend
1. Suggest backend
  
A typical production installation deploys the API and consumer on separate instances.

The rest of the getting started docs describes how to configure the various backends along with some best practices learned along the way.

 
## Metric Backend

There are 2 durable metric backends, Bigtable and Cassandra.

### Bigtable

#### Configuration

If you want to use Google Cloud Bigtable to store metrics, you can configure it with the following command using the the <a href="docs/shell">Heroic shell</a>. This will create tables and column families.

```bash
tools/heroic-shell -P bigtable \
  -X bigtable.project=<project> \
  -X bigtable.instance=<instance> \
  -X bigtable.credentials=default \
  -X bigtable.configure
...
heroic> configure
```

### Cassandra

#### Configuration

Heroic by default uses the `heroic` keyspace, which can be configured using the <a href="docs/shell">Heroic shell</a>.

```bash
tools/heroic-shell -P cassandra -X cassandra.seeds=<seeds> -X datastax.configure
...
heroic> configure
```

## Metadata/Suggest Backend

### Elasticsearch

Both the API and consumer nodes connect to Elasticsearch.

**warning** This is not a complete list of best practices of running Elasticsearch

When running Elasticsearch on k8s, a headless service should be configured in order for per-pod dns to be assigned and for cluster discovery to work. Sniffing should be enabled so as the k8s deployment is updated the new pods are discovered by Heroic. See [#477][3] for more info.

#### Configuration

Elasticsearch is also configured using the <a href="docs/shell">Heroic shell</a> or having `configure: true` in the config for the backend.

```bash
tools/heroic-shell -P elasticsearch-suggest -P elasticsearch-metadata -X elasticsearch.seeds=<seeds>
...
heroic> configure
```

## Consumer Backend

### Kafka

TODO: Add more here.

### PubSub

Pubsub is fully managed by Google. In order for the topics/subscriptions to be automatically created by Heroic is needs to have the proper PubSub IAM permissions to do so.


[1]: https://github.com/spotify/ffwd
[2]: https://github.com/spotify/heroic#docker
[3]: https://github.com/spotify/heroic/issues/477 
