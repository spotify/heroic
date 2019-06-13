/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.statistics.semantic;

import com.codahale.metrics.Counter;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.SuggestBackendReporter;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import com.spotify.heroic.suggest.WriteSuggest;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import eu.toolchain.async.AsyncFuture;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

public class SemanticSuggestBackendReporter implements SuggestBackendReporter {
    private static final String COMPONENT = "suggest-backend";
    private static final Tracer tracer = Tracing.getTracer();

    private final FutureReporter tagValuesSuggest;
    private final FutureReporter tagKeyCount;
    private final FutureReporter tagSuggest;
    private final FutureReporter keySuggest;
    private final FutureReporter tagValueSuggest;
    private final FutureReporter write;
    private final FutureReporter backendWrite;

    private final Counter writesDroppedByCacheHit;
    private final Counter writesDroppedByDuplicate;

    public SemanticSuggestBackendReporter(SemanticMetricRegistry registry) {
        final MetricId base = MetricId.build().tagged("component", COMPONENT);

        tagValuesSuggest = new SemanticFutureReporter(registry,
            base.tagged("what", "tag-values-suggest", "unit", Units.QUERY));
        tagKeyCount = new SemanticFutureReporter(registry,
            base.tagged("what", "tag-key-count", "unit", Units.QUERY));
        tagSuggest = new SemanticFutureReporter(registry,
            base.tagged("what", "tag-suggest", "unit", Units.QUERY));
        keySuggest = new SemanticFutureReporter(registry,
            base.tagged("what", "key-suggest", "unit", Units.QUERY));
        tagValueSuggest = new SemanticFutureReporter(registry,
            base.tagged("what", "tag-value-suggest", "unit", Units.QUERY));
        write =
            new SemanticFutureReporter(registry, base.tagged("what", "write", "unit", Units.WRITE));
        backendWrite = new SemanticFutureReporter(registry,
            base.tagged("what", "backend-write", "unit", Units.WRITE));

        writesDroppedByCacheHit = registry.counter(
            base.tagged("what", "writes-dropped-by-cache-hit", "unit", Units.COUNT));
        writesDroppedByDuplicate = registry.counter(
            base.tagged("what", "writes-dropped-by-duplicate", "unit", Units.COUNT));
    }

    @Override
    public SuggestBackend decorate(
        final SuggestBackend backend
    ) {
        return new InstrumentedSuggestBackend(backend);
    }

    @Override
    public void reportWriteDroppedByCacheHit() {
        writesDroppedByCacheHit.inc();
    }

    @Override
    public void reportWriteDroppedByDuplicate() {
        writesDroppedByDuplicate.inc();
    }

    @Override
    public FutureReporter.Context setupWriteReporter() {
        return backendWrite.setup();
    }

    public String toString() {
        return "SemanticSuggestBackendReporter()";
    }

    private class InstrumentedSuggestBackend implements SuggestBackend {
        private final SuggestBackend delegate;

        @java.beans.ConstructorProperties({ "delegate" })
        public InstrumentedSuggestBackend(final SuggestBackend delegate) {
            this.delegate = delegate;
        }

        @Override
        public AsyncFuture<Void> configure() {
            return null;
        }

        @Override
        public AsyncFuture<TagValuesSuggest> tagValuesSuggest(
            final TagValuesSuggest.Request request
        ) {
            return delegate.tagValuesSuggest(request).onDone(tagValuesSuggest.setup());
        }

        @Override
        public AsyncFuture<TagKeyCount> tagKeyCount(final TagKeyCount.Request request) {
            return delegate.tagKeyCount(request).onDone(tagKeyCount.setup());
        }

        @Override
        public AsyncFuture<TagSuggest> tagSuggest(final TagSuggest.Request request) {
            return delegate.tagSuggest(request).onDone(tagSuggest.setup());
        }

        @Override
        public AsyncFuture<KeySuggest> keySuggest(final KeySuggest.Request request) {
            return delegate.keySuggest(request).onDone(keySuggest.setup());
        }

        @Override
        public AsyncFuture<TagValueSuggest> tagValueSuggest(final TagValueSuggest.Request request) {
            return delegate.tagValueSuggest(request).onDone(tagValueSuggest.setup());
        }

        @Override
        public AsyncFuture<WriteSuggest> write(final WriteSuggest.Request request) {
            return write(request, tracer.getCurrentSpan());
        }

        @Override
        public AsyncFuture<WriteSuggest> write(
            final WriteSuggest.Request request, final Span parentSpan
        ) {
            return delegate.write(request, parentSpan).onDone(write.setup());
        }

        @Override
        public Statistics getStatistics() {
            return delegate.getStatistics();
        }

        @Override
        public boolean isReady() {
            return delegate.isReady();
        }

        @Override
        public Groups groups() {
            return delegate.groups();
        }

        @Override
        public String toString() {
            return delegate.toString() + "{semantic}";
        }
    }
}
