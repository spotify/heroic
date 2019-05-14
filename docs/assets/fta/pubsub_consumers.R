#!/usr/bin/env Rscript

library(FaultTree)

################################################################################
## Variables in cluster setup
################################################################################
elasticsearch_replication <- 2
elasticsearch_master_nodes <- 3
elasticsearch_data_nodes <- 40
## number of metrics ingested per second
metrics_per_second <- 1000000
## number of metrics each consumer can process per second
consumer_capacity <- 30000
## number of consumers. if using autoscaling, set this to the autoscaler max
consumers <- 35
################################################################################

capacity_max <- ceiling(consumers - (metrics_per_second / consumer_capacity))

tree <- ftree.make(type="or", name="PubSub consumer", name2="pipeline failure")

tree <- addLogic(tree, at=1, type="or", tag="MetadataCluster", name="Metadata consumers", name2="cluster failure")
tree <- addLogic(tree, at=1, type="or", tag="BigtableCluster", name="Bigtable consumer", name2="cluster failure")

## PubSub failure
## https://cloud.google.com/pubsub/sla
tree <- addProbability(tree, at="MetadataCluster", tag="PubSub", name="PubSub failure", prob=0.0005)

## Host failure events
tree <- addLogic(tree, at="MetadataCluster", type="vote", vote_par=c(capacity_max, consumers),
                 tag="Capacity", name="Consumer", name2="capacity exceeded")
tree <- addLogic(tree, at="Capacity", type="or", tag="Host", name="Host failure")
tree <- addProbability(tree, at="Host", tag="GCEInstance", name="GCE instance failure", prob=0.0001)
tree <- addProbability(tree, at="Host", tag="OS", name="OS fault", prob=0.0008)
tree <- addProbability(tree, at="Host", tag="Network", name="Network partition", prob=0.0017)

## Elasticsearch failures
tree <- addLogic(tree, at="MetadataCluster", type="or", tag="ElasticsearchCluster",
                 name="Elasticsearch", name2="cluster failure")
es_master_max <- ceiling(elasticsearch_master_nodes / 2)
tree <- addLogic(tree, at="ElasticsearchCluster", type="vote", vote_par=c(es_master_max, elasticsearch_master_nodes),
                 tag="ElasticsearchQuorum", name="Elasticsearch quorum", name2="majority loss")
tree <- addLogic(tree, at="ElasticsearchQuorum", type="or", tag="ElasticsearchMaster",
                 name="Elasticsearch master", name2="node failure")
tree <- addProbability(tree, at="ElasticsearchMaster", tag="Disk", name="Disk failure", prob=0.003)
tree <- addDuplicate(tree, at="ElasticsearchMaster", dup_id="Host", collapse=TRUE)
## Shard failure is not represented particularly accurately. A failure would only occur if the primary and all
## replicas failed, but the voting gate below assumes any K nodes failing will have the same effect.
tree <- addLogic(tree, at="ElasticsearchCluster", type="vote",
                 vote_par=c(elasticsearch_replication, elasticsearch_data_nodes),
                 tag="ElasticsearchShard", name="Elasticsearch shard", name2="failure")
tree <- addLogic(tree, at="ElasticsearchShard", type="or", tag="ElasticsearchData",
                 name="Elasticsearch data", name2="node failure")
tree <- addDuplicate(tree, at="ElasticsearchData", dup_id="Disk", collapse=TRUE)
tree <- addDuplicate(tree, at="ElasticsearchData", dup_id="Host", collapse=TRUE)

## Bigtable consumers
## https://cloud.google.com/bigtable/sla
tree <- addProbability(tree, at="BigtableCluster", tag="ZonalBT", name="Zonal cluster failure", prob=0.001)
tree <- addDuplicate(tree, at="BigtableCluster", dup_id="Capacity", collapse=TRUE)


## TODO: this displays all probabilities in scientic notation. It would be nice to make them into percentages.
tree <- ftree.calc(tree)
for (p in tree$PBF) {
    (1 - p)  * 100
}

## SCRAM will calculate probability through a Binary Decision Diagram algorithmn, giving the accepted exact
## probability. It will almost certainly differ than the number shown from `ftree.calc`.
##
## NB: using a voting logic gate prevents us from using SCRAM for calculations. We could
## use addAtLeast but that doesn't work with `ftree.calc`.
##
## TODO: output the scram results
## max_slo <- (1 - scram.probability(tree)) * 100
## importance <- scram.importance(tree)

## Output the result.
ftree2html(tree, write_file=TRUE)
## browseURL("tree.html")

## TODO: look into using other types for basic events:
## addExposed, addLatent, addDemand
