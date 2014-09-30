package com.spotify.heroic.aggregationcache.cassandra.filter;

import java.util.HashMap;

import com.netflix.astyanax.Serializer;
import com.spotify.heroic.ext.serializers.SafeStringSerializer;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.FalseFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.HasTagFilter;
import com.spotify.heroic.filter.ManyTermsFilter;
import com.spotify.heroic.filter.ManyTermsFilterBuilder;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.filter.NoTermFilter;
import com.spotify.heroic.filter.NoTermFilterBuilder;
import com.spotify.heroic.filter.NotFilter;
import com.spotify.heroic.filter.OneTermFilter;
import com.spotify.heroic.filter.OneTermFilterBuilder;
import com.spotify.heroic.filter.OrFilter;
import com.spotify.heroic.filter.RegexFilter;
import com.spotify.heroic.filter.StartsWithFilter;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.filter.TwoTermsFilter;
import com.spotify.heroic.filter.TwoTermsFilterBuilder;

public final class CassandraCommon {
    private static final HashMap<Integer, FilterSerialization<? extends Filter>> ID_TO_S = new HashMap<Integer, FilterSerialization<? extends Filter>>();
    private static final HashMap<Class<?>, Integer> TYPE_TO_S = new HashMap<Class<?>, Integer>();

    private static <T extends Filter> void register(int typeId, Class<T> type,
            FilterSerialization<T> serialization) {
        if (ID_TO_S.put(typeId, serialization) != null)
            throw new IllegalStateException("Multiple mappings for single id: "
                    + typeId);
        if (TYPE_TO_S.put(type, typeId) != null)
            throw new IllegalStateException(
                    "Multiple mappings for single type: " + type);
    }

    private static <T extends OneTermFilter<O>, O> void register(int typeId,
            Class<T> type, Serializer<O> serializer,
            OneTermFilterBuilder<T, O> builder) {
        register(typeId, type, new OneTermSerialization<>(serializer, builder));
    }

    private static <T extends TwoTermsFilter> void register(int typeId,
            Class<T> type, TwoTermsFilterBuilder<T> builder) {
        register(typeId, type, new TwoTermsSerialization<>(builder));
    }

    private static <T extends ManyTermsFilter> void register(int typeId,
            Class<T> type, ManyTermsFilterBuilder<T> builder) {
        register(typeId, type, new ManyTermsSerialization<>(builder));
    }

    private static <T extends NoTermFilter> void register(int typeId,
            Class<T> type, NoTermFilterBuilder<T> builder) {
        register(typeId, type, new NoTermSerialization<>(builder));
    }

    static {
        register(0x0001, AndFilter.class, AndFilter.BUILDER);
        register(0x0002, OrFilter.class, OrFilter.BUILDER);
        register(0x0003, NotFilter.class, FilterSerializer.get(),
                NotFilter.BUILDER);
        register(0x0010, MatchKeyFilter.class, SafeStringSerializer.get(),
                MatchKeyFilter.BUILDER);
        register(0x0011, MatchTagFilter.class, MatchTagFilter.BUILDER);
        register(0x0012, HasTagFilter.class, SafeStringSerializer.get(),
                HasTagFilter.BUILDER);
        register(0x0013, StartsWithFilter.class, StartsWithFilter.BUILDER);
        register(0x0014, RegexFilter.class, RegexFilter.BUILDER);
        register(0x0020, TrueFilter.class, TrueFilter.BUILDER);
        register(0x0021, FalseFilter.class, FalseFilter.BUILDER);
    }

    public static int getTypeId(Class<? extends Filter> type) {
        final Integer typeId = TYPE_TO_S.get(type);

        if (typeId == null)
            throw new RuntimeException("No serializer for type " + type);

        return typeId;
    }

    public static FilterSerialization<Filter> getSerializer(int typeId) {
        @SuppressWarnings("unchecked")
        final FilterSerialization<Filter> serializer = (FilterSerialization<Filter>) ID_TO_S
                .get(typeId);

        if (serializer == null)
            throw new RuntimeException("No serializer for type id "
                    + Integer.toHexString(typeId));

        return serializer;
    }
}
