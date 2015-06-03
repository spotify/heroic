package com.spotify.heroic.filter;

import eu.toolchain.serializer.Serializer;

public interface FilterSerializer extends Serializer<Filter> {
    public <T extends Filter.OneArg<A>, A> void register(String id, Class<T> type, OneArgumentFilter<T, A> builder,
            Serializer<A> first);

    public <T extends Filter.TwoArgs<A, B>, A, B> void register(String id, Class<T> type,
            TwoArgumentsFilter<T, A, B> builder, Serializer<A> first, Serializer<B> second);

    public <T extends Filter.MultiArgs<A>, A> void register(String id, Class<T> type,
            MultiArgumentsFilter<T, A> builder, Serializer<A> term);

    public <T extends Filter.NoArg> void register(String id, Class<T> type, NoArgumentFilter<T> builder);
}
