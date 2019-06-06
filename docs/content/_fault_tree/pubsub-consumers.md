---
components: Heroic consumers using PubSub for ingestion, and Bigtable and Elasticsearch for storage
---

Some assumptions are made that negatively affect the accuracy of the model and the calculated probabilities should be seen as being quite pessimistic. The biggest assumptions are:

- Numbers for Google Cloud services are taken from their SLAs. In reality, the services will typically be much more reliable than the numbers listed since SLAs are contractual agreements and skew conservatively.
- Elasticsearch shard failure is modeled as any n data nodes failing in the cluster, where n is the replication factor. The actual failure would have to be all replicas failing together, not just any random nodes.
