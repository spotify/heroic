package com.spotify.heroic.aggregation.simple

import com.spotify.heroic.aggregation.AbstractBucket
import com.spotify.heroic.aggregation.TDigestBucket
import com.spotify.heroic.metric.DistributionPoint
import com.spotify.heroic.metric.HeroicDistribution
import com.tdunning.math.stats.TDigest;
import java.util.concurrent.atomic.AtomicReference



data class TdigestStatBucket(override val timestamp: Long) : AbstractBucket(), TDigestBucket {
    private val datasketch : AtomicReference<TDigest> = TdigestStatInstanceUtils.buildAtomicReference()


    override fun updateDistributionPoint(key: Map<String, String>, sample : DistributionPoint) {
        val heroicDistribution : HeroicDistribution = HeroicDistribution.create(sample.value().value)
        val serializedDatasketch = heroicDistribution.toByteBuffer()
        datasketch.getAndUpdate(TdigestStatInstanceUtils.getOp(serializedDatasketch))
    }

    override fun value(): TDigest { //TODO check when this is call
        return datasketch.get()
    }
}
