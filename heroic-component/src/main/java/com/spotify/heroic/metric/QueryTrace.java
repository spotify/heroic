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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

@JsonSerialize(using = QueryTrace.Serializer.class)
@JsonDeserialize(using = QueryTrace.Deserializer.class)
public interface QueryTrace {
    Identifier PASSIVE_IDENTIFIER = Identifier.create("NO TRACE");
    PassiveTrace PASSIVE = PassiveTrace.create();
    NamedWatch PASSIVE_NAMED_WATCH = PassiveNamedWatch.create();
    Joiner PASSIVE_JOINER = new PassiveJoiner();

    /**
     * Create an active query trace.
     *
     * @param what Identifier of the trace
     * @return a {@link com.spotify.heroic.metric.QueryTrace}
     * @deprecated use {@link #watch(com.spotify.heroic.metric.QueryTrace.Identifier)}
     */
    static QueryTrace of(final Identifier what) {
        return ActiveTrace.create(what, 0L, ImmutableList.of());
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
        return ActiveTrace.create(what, elapsed, ImmutableList.of());
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
        return ActiveTrace.create(what, elapsed, children);
    }

    /**
     * Create a new identifier for a class.
     *
     * @param where Class associated with the identifier
     * @return an {@link com.spotify.heroic.metric.QueryTrace.Identifier}
     */
    static Identifier identifier(Class<?> where) {
        return Identifier.create(where.getName());
    }

    /**
     * Create a new identifier for a class and a what
     *
     * @param where Class associated with the identifier
     * @param what String describing what is being traced
     * @return an {@link com.spotify.heroic.metric.QueryTrace.Identifier}
     */
    static Identifier identifier(Class<?> where, String what) {
        return Identifier.create(where.getName() + "#" + what);
    }

    /**
     * Create a new identifier for a free text description.
     *
     * @param description Description telling what is being traced.
     * @return an {@link com.spotify.heroic.metric.QueryTrace.Identifier}
     */
    static Identifier identifier(String description) {
        return Identifier.create(description);
    }

    /**
     * Create a new watch.
     *
     * @return a {@link com.spotify.heroic.metric.QueryTrace.Watch}
     * @deprecated use {@link Tracing#watch()}
     */
    static Watch watch() {
        return ActiveWatch.create(Stopwatch.createStarted());
    }

    /**
     * Create a new watch.
     *
     * @return a {@link com.spotify.heroic.metric.QueryTrace.Watch}
     * @deprecated use {@link Tracing#watch(Identifier)}
     */
    static NamedWatch watch(final Identifier what) {
        return ActiveNamedWatch.create(what, Stopwatch.createStarted());
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
    @AutoValue
    abstract class PassiveTrace implements QueryTrace {
        public static PassiveTrace create() {
            return new AutoValue_QueryTrace_PassiveTrace(
                PASSIVE_IDENTIFIER, 0L, ImmutableList.of());
        }

        public abstract Identifier what();
        public abstract long elapsed();
        public abstract List<QueryTrace> children();

        @Override
        public void formatTrace(final PrintWriter out) {
            formatTrace(out, "");
        }

        @Override
        public void formatTrace(final PrintWriter out, final String prefix) {
            out.println(prefix + "NO TRACE");
        }
    }

    @JsonTypeName("active")
    @AutoValue
    abstract class ActiveTrace implements QueryTrace {
        public static ActiveTrace create(Identifier what, long elapsed, List<QueryTrace> children) {
            return new AutoValue_QueryTrace_ActiveTrace(what, elapsed, children);
        }

        public abstract Identifier what();
        public abstract long elapsed();
        public abstract List<QueryTrace> children();

        @Override
        public void formatTrace(PrintWriter out) {
            formatTrace(out, "");
        }

        @Override
        public void formatTrace(PrintWriter out, String prefix) {
            out.println(prefix + what() + " (in " + readableTime(elapsed()) + ")");

            for (final QueryTrace child : children()) {
                child.formatTrace(out, prefix + "  ");
            }
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
            return ActiveJsonModel.create(what(), elapsed(), children());
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
    @AutoValue
    abstract class ActiveWatch implements Watch {
        static ActiveWatch create(Stopwatch watch) {
            return new AutoValue_QueryTrace_ActiveWatch(watch);
        }

        abstract Stopwatch watch();

        @Override
        public final QueryTrace end(final Identifier what) {
            return ActiveTrace.create(what, elapsed(), ImmutableList.of());
        }

        @Override
        public final QueryTrace end(final Identifier what, final QueryTrace child) {
            return ActiveTrace.create(what, elapsed(), ImmutableList.of(child));
        }

        @Override
        public final QueryTrace end(final Identifier what, final List<QueryTrace> children) {
            return ActiveTrace.create(what, elapsed(), children);
        }

        @Override
        public final long elapsed() {
            return watch().elapsed(TimeUnit.MICROSECONDS);
        }
    }

    /**
     * A watch that is not measuring.
     */
    @AutoValue
    abstract class PassiveWatch implements Watch {
        static PassiveWatch create() {
            return new AutoValue_QueryTrace_PassiveWatch();
        }

        @Override
        public final QueryTrace end(final Identifier what) {
            return PASSIVE;
        }

        @Override
        public final QueryTrace end(final Identifier what, final QueryTrace child) {
            return PASSIVE;
        }

        @Override
        public final QueryTrace end(final Identifier what, final List<QueryTrace> children) {
            return PASSIVE;
        }

        @Override
        public final long elapsed() {
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

    @AutoValue
    abstract class ActiveNamedWatch implements NamedWatch {
        static ActiveNamedWatch create(Identifier what, Stopwatch watch) {
            return new AutoValue_QueryTrace_ActiveNamedWatch(what, watch);
        }

        abstract Identifier what();
        abstract Stopwatch watch();

        @Override
        public final QueryTrace end() {
            return ActiveTrace.create(what(), elapsed(), ImmutableList.of());
        }

        @Override
        public final QueryTrace end(final QueryTrace child) {
            return ActiveTrace.create(what(), elapsed(), ImmutableList.of(child));
        }

        @Override
        public final QueryTrace end(final List<QueryTrace> children) {
            return ActiveTrace.create(what(), elapsed(), children);
        }

        @Override
        public final Joiner joiner() {
            return ActiveJoiner.create(this);
        }

        @Override
        public final NamedWatch watch(final Identifier what) {
            return ActiveNamedWatch.create(what, Stopwatch.createStarted());
        }

        @Override
        public final NamedWatch extendIdentifier(String appendName) {
            return ActiveNamedWatch.create(what().extend(appendName), watch());
        }

        @Override
        public final long elapsed() {
            return watch().elapsed(TimeUnit.MICROSECONDS);
        }
    }

    @AutoValue
    abstract class PassiveNamedWatch implements NamedWatch {
        static PassiveNamedWatch create() {
            return new AutoValue_QueryTrace_PassiveNamedWatch();
        }

        @Override
        public final QueryTrace end() {
            return PASSIVE;
        }

        @Override
        public final QueryTrace end(final QueryTrace child) {
            return PASSIVE;
        }

        @Override
        public final QueryTrace end(final List<QueryTrace> children) {
            return PASSIVE;
        }

        @Override
        public final Joiner joiner() {
            return PASSIVE_JOINER;
        }

        @Override
        public final NamedWatch watch(final Identifier what) {
            return PASSIVE_NAMED_WATCH;
        }

        @Override
        public final NamedWatch extendIdentifier(String appendName) {
            return PASSIVE_NAMED_WATCH;
        }

        @Override
        public final long elapsed() {
            return 0L;
        }
    }

    @AutoValue
    abstract class Identifier {
        @JsonCreator
        public static Identifier create(@JsonProperty("name") String name) {
            return new AutoValue_QueryTrace_Identifier(name);
        }

        @JsonProperty
        public abstract String name();

        public final Identifier extend(String key) {
            return Identifier.create(name() + "[" + key + "]");
        }

        public final String toString() {
            return name();
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
    @AutoValue
    abstract class ActiveJoiner implements Joiner {
        static ActiveJoiner create(QueryTrace.NamedWatch watch) {
            return new AutoValue_QueryTrace_ActiveJoiner(watch);
        }

        abstract QueryTrace.NamedWatch watch();

        private final ImmutableList.Builder<QueryTrace> children = ImmutableList.builder();

        @Override
        public final void addChild(final QueryTrace trace) {
            children.add(trace);
        }

        @Override
        public final QueryTrace result() {
            return watch().end(children.build());
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
        public QueryTrace getNullValue(final DeserializationContext ctxt) {
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
    @AutoValue
    abstract class ActiveJsonModel {

        @JsonCreator
        public static ActiveJsonModel create(
            @JsonProperty("what") Identifier what,
            @JsonProperty("elapsed") long elapsed,
            @JsonProperty("children") List<QueryTrace> children
        ) {
            return new AutoValue_QueryTrace_ActiveJsonModel(what, elapsed, children);
        }

        @JsonProperty
        public abstract Identifier what();

        @JsonProperty
        public abstract long elapsed();

        @JsonProperty
        public abstract List<QueryTrace> children();

        /**
         * Convert to active instance.
         *
         * @return a new active instance
         */
        final QueryTrace toActive() {
            return ActiveTrace.create(what(), elapsed(), children());
        }
    }
}
