package com.spotify.heroic.aggregation.simple

import com.spotify.heroic.aggregation.AbstractBucket
import com.spotify.heroic.aggregation.TDigestBucket
import com.spotify.heroic.metric.DistributionPoint
import com.spotify.heroic.metric.HeroicDistribution
import com.spotify.heroic.metric.TdigestPoint
import com.tdunning.math.stats.TDigest;
import java.util.concurrent.atomic.AtomicReference

/**
 *
 * This bucket merges data sketch in every distribution data point visited.
 * As the name indicates, this implementation only supports Tdigest.
 *
 */
data class TdigestMergingBucket(override val timestamp: Long) : AbstractBucket(), TDigestBucket {
    private val datasketch : AtomicReference<TDigest> = TdigestStatInstanceUtils.buildAtomicReference()


    override fun updateDistributionPoint(key: Map<String, String>, sample : DistributionPoint) {
        val heroicDistribution : HeroicDistribution = HeroicDistribution.create(sample.value().value)
        val serializedDatasketch = heroicDistribution.toByteBuffer()
        datasketch.getAndUpdate(TdigestStatInstanceUtils.getOp(serializedDatasketch))
    }

    override fun updateTDigestPoint(key: Map<String, String>, sample : TdigestPoint) {
        datasketch.getAndUpdate(TdigestStatInstanceUtils.getOp(sample.value()))
    }

    override fun value(): TDigest {
        return datasketch.get()
    }
}
