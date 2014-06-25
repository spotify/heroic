package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.codahale.metrics.Meter;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.heroic.statistics.HeroicTimer;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticCallbackReporter implements CallbackReporter {
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

    public SemanticCallbackReporter(SemanticMetricRegistry registry, MetricId id) {
        this.timer = new SemanticHeroicTimer(registry.timer(id));
        this.cancelled = registry.meter(id.tagged("unit", Units.CANCELS));
        this.failed = registry.meter(id.tagged("unit", Units.FAILURES));
        this.resolved = registry.meter(id.tagged("unit", Units.RESOLVES));
    }

    @Override
    public Context setup() {
        return new SemanticContext(timer.time());
    }
}
