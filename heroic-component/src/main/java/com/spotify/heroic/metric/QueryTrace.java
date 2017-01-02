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

package com.spotify.heroic.metric;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import lombok.Data;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

@JsonSerialize(using = QueryTrace.Serializer.class)
@JsonDeserialize(using = QueryTrace.Deserializer.class)
public interface QueryTrace {
    Identifier PASSIVE_IDENTIFIER = new Identifier("NO TRACE");
    PassiveTrace PASSIVE = new PassiveTrace();
    Watch PASSIVE_WATCH = new PassiveWatch();
    NamedWatch PASSIVE_NAMED_WATCH = new PassiveNamedWatch();
    Joiner PASSIVE_JOINER = new PassiveJoiner();

    /**
     * Create an active query trace.
     *
     * @param what Identifier of the trace
     * @return a {@link com.spotify.heroic.metric.QueryTrace}
     * @deprecated use {@link #watch(com.spotify.heroic.metric.QueryTrace.Identifier)}
     */
    static QueryTrace of(final Identifier what) {
        return new ActiveTrace(what, 0L, ImmutableList.of());
    }

    /**
     * Create an active query trace.
     *
     * @param what Identifier of the trace
     * @param elapsed How long the query trace has elapsed for
     * @return a {@link com.spotify.heroic.metric.QueryTrace}
     * @deprecated use {@link #watch(com.spotify.heroic.metric.QueryTrace.Identifier)}
     */
    static QueryTrace of(final Identifier what, final long elapsed) {
        return new ActiveTrace(what, elapsed, ImmutableList.of());
    }

    /**
     * Create an active query trace.
     *
     * @param what Identifier of the trace
     * @param elapsed How long the query trace has elapsed for
     * @param children Children of the query trace
     * @return a {@link com.spotify.heroic.metric.QueryTrace}
     * @deprecated use {@link #watch(com.spotify.heroic.metric.QueryTrace.Identifier)}
     */
    static QueryTrace of(
        final Identifier what, final long elapsed, final List<QueryTrace> children
    ) {
        return new ActiveTrace(what, elapsed, children);
    }

    /**
     * Create a new identifier for a class.
     *
     * @param where Class associated with the identifier
     * @return an {@link com.spotify.heroic.metric.QueryTrace.Identifier}
     */
    static Identifier identifier(Class<?> where) {
        return new Identifier(where.getName());
    }

    /**
     * Create a new identifier for a class and a what
     *
     * @param where Class associated with the identifier
     * @param what String describing what is being traced
     * @return an {@link com.spotify.heroic.metric.QueryTrace.Identifier}
     */
    static Identifier identifier(Class<?> where, String what) {
        return new Identifier(where.getName() + "#" + what);
    }

    /**
     * Create a new identifier for a free text description.
     *
     * @param description Description telling what is being traced.
     * @return an {@link com.spotify.heroic.metric.QueryTrace.Identifier}
     */
    static Identifier identifier(String description) {
        return new Identifier(description);
    }

    /**
     * Create a new watch.
     *
     * @return a {@link com.spotify.heroic.metric.QueryTrace.Watch}
     * @deprecated use {@link Tracing#watch()}
     */
    static Watch watch() {
        return new ActiveWatch(Stopwatch.createStarted());
    }

    /**
     * Create a new watch.
     *
     * @return a {@link com.spotify.heroic.metric.QueryTrace.Watch}
     * @deprecated use {@link Tracing#watch(Identifier)}
     */
    static NamedWatch watch(final Identifier what) {
        return new ActiveNamedWatch(what, Stopwatch.createStarted());
    }

    /**
     * Format the current trace onto the given PrintWriter.
     *
     * @param out {@link java.io.PrintWriter} to format to
     */
    void formatTrace(PrintWriter out);

    /**
     * Format the current trace onto the given PrintWriter with the given prefix.
     *
     * @param out {@link java.io.PrintWriter} to format to
     * @param prefix prefix to prepend
     */
    void formatTrace(PrintWriter out, String prefix);

    /**
     * @deprecated use {@link #elapsed()}
     */
    @Deprecated
    long getElapsed();

    /**
     * @deprecated use {@link #what()}
     */
    @Deprecated
    Identifier getWhat();

    /**
     * @deprecated use {@link #children()}
     */
    @Deprecated
    List<QueryTrace> getChildren();

    /**
     * How long the trace elapsed for.
     *
     * @return microseconds
     */
    long elapsed();

    /**
     * Get the identifier for this trace.
     *
     * @return an identifier
     */
    Identifier what();

    /**
     * Get a list of all immediate children to this trace.
     *
     * @return a list
     */
    List<QueryTrace> children();

    @JsonTypeName("passive")
    @Data
    class PassiveTrace implements QueryTrace {
        PassiveTrace() {
        }

        @JsonCreator
        public static PassiveTrace create() {
            return PASSIVE;
        }

        @Override
        public void formatTrace(final PrintWriter out) {
            formatTrace(out, "");
        }

        @Override
        public void formatTrace(final PrintWriter out, final String prefix) {
            out.println(prefix + "NO TRACE");
        }

        @JsonIgnore
        @Override
        public long getElapsed() {
            return elapsed();
        }

        @JsonIgnore
        @Override
        public Identifier getWhat() {
            return what();
        }

        @JsonIgnore
        @Override
        public List<QueryTrace> getChildren() {
            return children();
        }

        @Override
        public long elapsed() {
            return 0L;
        }

        @Override
        public Identifier what() {
            return PASSIVE_IDENTIFIER;
        }

        @Override
        public List<QueryTrace> children() {
            return ImmutableList.of();
        }
    }

    @JsonTypeName("active")
    @Data
    class ActiveTrace implements QueryTrace {
        private final Identifier what;
        private final long elapsed;
        private final List<QueryTrace> children;

        @Override
        public void formatTrace(PrintWriter out) {
            formatTrace(out, "");
        }

        @Override
        public void formatTrace(PrintWriter out, String prefix) {
            out.println(prefix + what + " (in " + readableTime(elapsed) + ")");

            for (final QueryTrace child : children) {
                child.formatTrace(out, prefix + "  ");
            }
        }

        @Override
        public long elapsed() {
            return elapsed;
        }

        @Override
        public Identifier what() {
            return what;
        }

        @Override
        public List<QueryTrace> children() {
            return children;
        }

        private String readableTime(long elapsed) {
            if (elapsed > 1000000000) {
                return String.format("%.3fs", elapsed / 1000000000d);
            }

            if (elapsed > 1000000) {
                return String.format("%.3fms", elapsed / 1000000d);
            }

            if (elapsed > 1000) {
                return String.format("%.3fus", elapsed / 1000d);
            }

            return elapsed + "ns";
        }

        /**
         * Convert to model.
         *
         * @return a new json model
         */
        ActiveJsonModel toModel() {
            return new ActiveJsonModel(what, elapsed, children);
        }
    }

    interface Watch {
        /**
         * End the current watch and return a trace.
         * <p>
         * The same watch can be used multiple times, weven when ended.
         *
         * @param what The thing that is being traced
         * @return a {@link com.spotify.heroic.metric.QueryTrace}
         */
        QueryTrace end(final Identifier what);

        /**
         * End the current watch and return a trace with the given child.
         * <p>
         * The same watch can be used multiple times, weven when ended.
         *
         * @param what The thing that is being traced
         * @param child Child to add to the new {@link com.spotify.heroic.metric.QueryTrace}
         * @return a {@link com.spotify.heroic.metric.QueryTrace}
         */
        QueryTrace end(final Identifier what, final QueryTrace child);

        /**
         * End the current watch and return a trace with the given children.
         * <p>
         * The same watch can be used multiple times, weven when ended.
         *
         * @param what The thing that is being traced
         * @param children Children to add to the new {@link com.spotify.heroic.metric.QueryTrace}
         * @return a {@link com.spotify.heroic.metric.QueryTrace}
         */
        QueryTrace end(final Identifier what, final List<QueryTrace> children);

        /**
         * How long this trace has elapsed for.
         *
         * @return microseconds
         * @deprecated Makes not distinction between passive and active watches
         */
        long elapsed();
    }

    /**
     * A watch that is measuring.
     */
    @Data
    class ActiveWatch implements Watch {
        private final Stopwatch w;

        @Override
        public QueryTrace end(final Identifier what) {
            return new ActiveTrace(what, elapsed(), ImmutableList.of());
        }

        @Override
        public QueryTrace end(final Identifier what, final QueryTrace child) {
            return new ActiveTrace(what, elapsed(), ImmutableList.of(child));
        }

        @Override
        public QueryTrace end(final Identifier what, final List<QueryTrace> children) {
            return new ActiveTrace(what, elapsed(), children);
        }

        @Override
        public long elapsed() {
            return w.elapsed(TimeUnit.MICROSECONDS);
        }
    }

    /**
     * A watch that is not measuring.
     */
    @Data
    class PassiveWatch implements Watch {
        @Override
        public QueryTrace end(final Identifier what) {
            return PASSIVE;
        }

        @Override
        public QueryTrace end(final Identifier what, final QueryTrace child) {
            return PASSIVE;
        }

        @Override
        public QueryTrace end(final Identifier what, final List<QueryTrace> children) {
            return PASSIVE;
        }

        @Override
        public long elapsed() {
            return 0L;
        }
    }

    interface NamedWatch {
        /**
         * End the current watch and return a trace.
         * <p>
         * The same watch can be used multiple times, weven when ended.
         *
         * @return a {@link com.spotify.heroic.metric.QueryTrace}
         */
        QueryTrace end();

        /**
         * End the current watch and return a trace with the given child.
         * <p>
         * The same watch can be used multiple times, weven when ended.
         *
         * @param child Child to add to the new {@link com.spotify.heroic.metric.QueryTrace}
         * @return a {@link com.spotify.heroic.metric.QueryTrace}
         */
        QueryTrace end(QueryTrace child);

        /**
         * End the current watch and return a trace with the given children.
         * <p>
         * The same watch can be used multiple times, weven when ended.
         *
         * @param children Children to add to the new {@link com.spotify.heroic.metric.QueryTrace}
         * @return a {@link com.spotify.heroic.metric.QueryTrace}
         */
        QueryTrace end(List<QueryTrace> children);

        /**
         * Create a joiner from the current watch.
         *
         * @return a {@link com.spotify.heroic.metric.QueryTrace.Joiner}
         */
        Joiner joiner();

        /**
         * Create another watch with the same configuration as this watch (active or passive).
         *
         * @param what What is being watched
         * @return a {@link com.spotify.heroic.metric.QueryTrace.NamedWatch}
         */
        NamedWatch watch(Identifier what);

        /**
         * Create another watch that copies everything from this watch, while extending the id.
         *
         * @param appendName What to extend the identifier with
         * @return a {@link com.spotify.heroic.metric.QueryTrace.NamedWatch}
         */
        NamedWatch extendIdentifier(String appendName);

        /**
         * How long this trace has elapsed for.
         *
         * @return milliseconds since unix epoch
         * @deprecated Makes no distinction between active and passive watches.
         */
        long elapsed();
    }

    @Data
    class ActiveNamedWatch implements NamedWatch {
        private final Identifier what;
        private final Stopwatch w;

        @Override
        public QueryTrace end() {
            return new ActiveTrace(what, elapsed(), ImmutableList.of());
        }

        @Override
        public QueryTrace end(final QueryTrace child) {
            return new ActiveTrace(what, elapsed(), ImmutableList.of(child));
        }

        @Override
        public QueryTrace end(final List<QueryTrace> children) {
            return new ActiveTrace(what, elapsed(), children);
        }

        @Override
        public Joiner joiner() {
            return new ActiveJoiner(this);
        }

        @Override
        public NamedWatch watch(final Identifier what) {
            return new ActiveNamedWatch(what, Stopwatch.createStarted());
        }

        @Override
        public NamedWatch extendIdentifier(String appendName) {
            return new ActiveNamedWatch(what.extend(appendName), w);
        }

        @Override
        public long elapsed() {
            return w.elapsed(TimeUnit.MICROSECONDS);
        }
    }

    @Data
    class PassiveNamedWatch implements NamedWatch {
        @Override
        public QueryTrace end() {
            return PASSIVE;
        }

        @Override
        public QueryTrace end(final QueryTrace child) {
            return PASSIVE;
        }

        @Override
        public QueryTrace end(final List<QueryTrace> children) {
            return PASSIVE;
        }

        @Override
        public Joiner joiner() {
            return PASSIVE_JOINER;
        }

        @Override
        public NamedWatch watch(final Identifier what) {
            return PASSIVE_NAMED_WATCH;
        }

        @Override
        public NamedWatch extendIdentifier(String appendName) {
            return PASSIVE_NAMED_WATCH;
        }

        @Override
        public long elapsed() {
            return 0L;
        }
    }

    @Data
    class Identifier {
        private final String name;

        @JsonCreator
        public Identifier(@JsonProperty("name") String name) {
            this.name = name;
        }

        public Identifier extend(String key) {
            return new Identifier(name + "[" + key + "]");
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Joiner multiple query traces into one.
     */
    interface Joiner {
        /**
         * Add a child trace that should be part of the resulting trace.
         *
         * @param trace Child trace to add
         */
        void addChild(QueryTrace trace);

        /**
         * Create a new trace with a snapshot of the current list of children.
         *
         * @return a {@link com.spotify.heroic.metric.QueryTrace}
         */
        QueryTrace result();
    }

    /**
     * A joiner that joins the given children into a single trace.
     */
    @Data
    class ActiveJoiner implements Joiner {
        private final QueryTrace.NamedWatch watch;

        private final ImmutableList.Builder<QueryTrace> children = ImmutableList.builder();

        @Override
        public void addChild(final QueryTrace trace) {
            children.add(trace);
        }

        @Override
        public QueryTrace result() {
            return watch.end(children.build());
        }
    }

    /**
     * A joiner that does nothing.
     */
    class PassiveJoiner implements Joiner {
        PassiveJoiner() {
        }

        @Override
        public void addChild(final QueryTrace trace) {
            /* do nothing */
        }

        @Override
        public QueryTrace result() {
            return PASSIVE;
        }
    }

    class Serializer extends JsonSerializer<QueryTrace> {
        @Override
        public void serialize(
            final QueryTrace value, final JsonGenerator gen, final SerializerProvider serializers
        ) throws IOException {
            if (value instanceof PassiveTrace) {
                gen.writeNull();
                return;
            }

            gen.writeObject(((ActiveTrace) value).toModel());
        }
    }

    class Deserializer extends JsonDeserializer<QueryTrace> {
        @Override
        public QueryTrace getNullValue(final DeserializationContext ctxt)
            throws JsonMappingException {
            return PASSIVE;
        }

        @Override
        public QueryTrace deserialize(
            final JsonParser p, final DeserializationContext ctxt
        ) throws IOException {
            return p.readValueAs(ActiveJsonModel.class).toActive();
        }
    }

    /**
     * Intermediate JSON model used for serializing and deserializing active values.
     * <p>
     * <p>Attempting to immediately serialize something extending
     * {@link com.spotify.heroic.metric.QueryTrace} would cause infinite recursion since the same
     * serializer would be called over and over. This model breaks that.
     */
    @Data
    class ActiveJsonModel {
        private final Identifier what;
        private final long elapsed;
        private final List<QueryTrace> children;

        /**
         * Convert to active instance.
         *
         * @return a new active instance
         */
        QueryTrace toActive() {
            return new ActiveTrace(what, elapsed, children);
        }
    }
}
