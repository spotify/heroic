package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.codahale.metrics.Meter;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.HeroicTimer;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticFutureReporter implements FutureReporter {
    private final SemanticHeroicTimer timer;
    private final Meter cancelled;
    private final Meter failed;
    private final Meter resolved;

    @RequiredArgsConstructor
    private class SemanticContext implements Context {
        private final HeroicTimer.Context context;

        @Override
        public void failed(Exception e) throws Exception {
            failed.mark();
            context.stop();
        }

        @Override
        public void resolved(Object result) throws Exception {
            resolved.mark();
            context.stop();
        }

        @Override
        public void cancelled(CancelReason reason) throws Exception {
            cancelled.mark();
            context.stop();
        }
    }

    public SemanticFutureReporter(SemanticMetricRegistry registry, MetricId id) {
        final String what = id.getTags().get("what");

        if (what == null)
            throw new IllegalArgumentException("id does not provide the tag 'what'");

        this.timer = new SemanticHeroicTimer(registry.timer(id.tagged("what", what + "-latency")));
        this.cancelled = registry.meter(id.tagged("what", what + "-cancel-rate", "unit", Units.CANCEL));
        this.failed = registry.meter(id.tagged("what", what + "-failure-rate", "unit", Units.FAILURE));
        this.resolved = registry.meter(id.tagged("what", what + "-resolve-rate", "unit", Units.RESOLVE));
    }

    @Override
    public Context setup() {
        return new SemanticContext(timer.time());
    }
}
