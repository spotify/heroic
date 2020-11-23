package com.spotify.heroic.aggregation.simple

import com.spotify.heroic.aggregation.AbstractBucket
import com.spotify.heroic.aggregation.TDigestBucket
import com.spotify.heroic.metric.DistributionPoint
import com.spotify.heroic.metric.HeroicDistribution
import com.tdunning.math.stats.MergingDigest
import com.tdunning.math.stats.TDigest;


data class TdigestStatBucket(override val timestamp: Long) : AbstractBucket(), TDigestBucket {
    private val value : TDigest = TDigest.createMergingDigest(100.0)

    override fun updateDistributionPoint(key: Map<String, String>, sample : DistributionPoint) {
        val heroicDistribution : HeroicDistribution = HeroicDistribution.create(sample.value().value)
        value.add(MergingDigest.fromBytes(heroicDistribution.toByteBuffer())) // this has to be thread safe
    }

    override fun value(): TDigest {
        return value //what happen if we return an empty digest
    }
}
