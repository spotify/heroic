package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.codahale.metrics.Timer;
import com.spotify.heroic.statistics.HeroicTimer;

@RequiredArgsConstructor
public class SemanticHeroicTimer implements HeroicTimer {
    @RequiredArgsConstructor
    public class SemanticContext implements Context {
        private final Timer.Context context;

        @Override
        public void finished() throws Exception {
            stop();
        }

        @Override
        public long stop() {
            return context.stop();
        }
    }

    private final Timer timer;

    @Override
    public Context time() {
        return new SemanticContext(timer.time());
    }
}
