package com.spotify.heroic.aggregation.simple

import com.spotify.heroic.aggregation.AggregationContext
import com.spotify.heroic.aggregation.SamplingAggregation
import com.spotify.heroic.aggregation.SamplingQuery
import com.spotify.heroic.common.Duration

data class TdigestStat(
        val sampling: SamplingQuery?,
        override var size: Duration?,
        override var extent: Duration?
) : SamplingAggregation {

    init {
        size = size ?: sampling?.size
        extent = extent ?: sampling?.extent
    }

    override fun apply(context: AggregationContext?, size: Long, extent: Long): TdigestStatInstance {
        val quantiles :DoubleArray = DEFAULT_QUANTILES
        return TdigestStatInstance(size, extent, quantiles )
    }

    companion object {
        const val NAME = "tdigeststat"
         val DEFAULT_QUANTILES  = doubleArrayOf(0.5,0.75,0.99)
    }
}
