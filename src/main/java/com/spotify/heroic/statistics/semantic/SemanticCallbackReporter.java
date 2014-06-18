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
    private final Meter cancel;
    private final Meter error;
    private final Meter finish;

    @RequiredArgsConstructor
    private class SemanticContext implements Context {
        private final HeroicTimer.Context context;

        @Override
        public void failed(Exception e) throws Exception {
            error.mark();
            context.stop();
        }

        @Override
        public void resolved(Object result) throws Exception {
            finish.mark();
            context.stop();
        }

        @Override
        public void cancelled(CancelReason reason) throws Exception {
            cancel.mark();
            context.stop();
        }
    }

    public SemanticCallbackReporter(SemanticMetricRegistry registry, MetricId id) {
        this.timer = new SemanticHeroicTimer(registry.timer(id.tagged("what", "timer")));
        this.cancel = registry.meter(id.tagged("what", "meter", "result", "cancel"));
        this.error = registry.meter(id.tagged("what", "meter", "result", "error"));
        this.finish = registry.meter(id.tagged("what", "meter", "result", "finish"));
    }

    @Override
    public Context setup() {
        return new SemanticContext(timer.time());
    }
}
