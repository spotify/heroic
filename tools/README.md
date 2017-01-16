# Tools for working with Heroic

## querylog

A utility used for configuring an Elasticsearch cluster with the templates necessary to index
query logs as emitted by Heroic.

#### Usage

```bash
tools/querylog --host http://<elasticsearch-master>:9200 [--base-index <name>] [--logstash] \
  --commit
```

The value provided to the `--base-index` option is what will be used as a prefix to the index
template.
