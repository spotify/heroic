package com.spotify.heroic;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationBuilder;
import com.spotify.heroic.aggregation.AggregationQuery;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.MultiArgumentsFilter;
import com.spotify.heroic.filter.NoArgumentFilter;
import com.spotify.heroic.filter.OneArgumentFilter;
import com.spotify.heroic.filter.TwoArgumentsFilter;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.serializer.Serializer;

public interface HeroicContext {
    <T extends Aggregation, R extends AggregationQuery<T>> void aggregation(String id, Class<T> type,
            Class<R> queryType, Serializer<T> serializer, AggregationBuilder<T> builder);

    public <T extends Filter.OneArg<A>, I extends T, A> void filter(String typeId, Class<T> type, Class<I> impl,
            OneArgumentFilter<T, A> builder, Serializer<A> first);

    public <T extends Filter.TwoArgs<A, B>, I extends T, A, B> void filter(String typeId, Class<T> type, Class<I> impl,
            TwoArgumentsFilter<T, A, B> builder, Serializer<A> first, Serializer<B> second);

    public <T extends Filter.MultiArgs<A>, I extends T, A> void filter(String typeId, Class<T> type, Class<I> impl,
            MultiArgumentsFilter<T, A> builder, Serializer<A> term);

    public <T extends Filter.NoArg, I extends T> void filter(String typeId, Class<T> type, Class<I> impl,
            NoArgumentFilter<T> builder);

    /**
     * Future that will be resolved after all services have been started.
     */
    public AsyncFuture<Void> startedFuture();
}
