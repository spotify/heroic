package com.spotify.heroic.tracing

import io.opencensus.trace.*
import org.junit.Before
import org.junit.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RandomParentSamplerTest {

    lateinit var sampler: Sampler
    @Before
    fun setup() {
         sampler = RandomParentSampler(1.0)
    }

    @Test
    fun propagateNoSampleDownstream() {
        val spanContext = SpanContext.create(
            TraceId.INVALID,
            SpanId.INVALID,
            TraceOptions.builder().setIsSampled(false).build(),
            Tracestate.builder().build())

        assertFalse {  sampler.shouldSample(spanContext, null, null, null, null, null) }
    }

    @Test
    fun sampleWhenProbabilityIsOne() {
        val spanContext = SpanContext.create(
            TraceId.INVALID,
            SpanId.INVALID,
            TraceOptions.builder().setIsSampled(true).build(),
            Tracestate.builder().build())

        assertTrue { sampler.shouldSample(spanContext, null, null, null, null, null) }
    }

    @Test
    fun invalidProbability() {
        assertFailsWith<IllegalArgumentException> { RandomParentSampler(500.0) }
    }
}
