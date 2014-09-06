package com.spotify.heroic.cache.cassandra.filter;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.spotify.heroic.cache.cassandra.model.CacheKey;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.filter.OrFilter;
import com.spotify.heroic.filter.RegexFilter;
import com.spotify.heroic.filter.StartsWithFilter;

/**
 * Serializes aggregation configurations.
 *
 * Each aggregation configuration is packed into a Composite which has the type
 * of the aggregation as a prefixed short.
 *
 * @author udoprog
 */
public class FilterSerializer extends AbstractSerializer<Filter> {
    private static final FilterSerializer instance = new FilterSerializer();

    public static FilterSerializer get() {
        return instance;
    }

    private static final IntegerSerializer integerSerializer = IntegerSerializer
            .get();

    private static final ByteBuffer ZERO_BUFFER = ByteBuffer.allocate(0);

    private static final int AND_ID = 0x0001;
    private static final int OR_ID = 0x0002;
    private static final int MATCH_KEY_ID = 0x0010;
    private static final int MATCH_TAG_ID = 0x0011;
    private static final int STARTS_WITH_ID = 0x0012;
    private static final int REGEX_ID = 0x0013;

    private static final FilterSerialization<MatchKeyFilter> MATCH_KEY = new OneTermSerialization<MatchKeyFilter>() {
        @Override
        protected MatchKeyFilter build(String value) {
            return new MatchKeyFilter(value);
        }
    };

    private static final FilterSerialization<MatchTagFilter> MATCH_TAG = new TwoTermsSerialization<MatchTagFilter>() {
        @Override
        protected MatchTagFilter build(String key, String value) {
            return new MatchTagFilter(key, value);
        }
    };

    private static final FilterSerialization<StartsWithFilter> STARTS_WITH = new TwoTermsSerialization<StartsWithFilter>() {
        @Override
        protected StartsWithFilter build(String key, String value) {
            return new StartsWithFilter(key, value);
        }
    };

    private static final FilterSerialization<RegexFilter> REGEX = new TwoTermsSerialization<RegexFilter>() {
        @Override
        protected RegexFilter build(String key, String value) {
            return new RegexFilter(key, value);
        }
    };

    private static final FilterSerialization<AndFilter> AND = new ManyTermsSerialization<AndFilter>() {
        @Override
        protected AndFilter build(List<Filter> statements) {
            return new AndFilter(statements);
        }
    };

    private static final FilterSerialization<OrFilter> OR = new ManyTermsSerialization<OrFilter>() {
        @Override
        protected OrFilter build(List<Filter> statements) {
            return new OrFilter(statements);
        }
    };

    private static final HashMap<Integer, FilterSerialization<? extends Filter>> ID_TO_S = new HashMap<Integer, FilterSerialization<? extends Filter>>();
    private static final HashMap<Class<?>, Integer> TYPE_TO_S = new HashMap<Class<?>, Integer>();

    static {
        ID_TO_S.put(AND_ID, AND);
        ID_TO_S.put(OR_ID, OR);
        ID_TO_S.put(MATCH_KEY_ID, MATCH_KEY);
        ID_TO_S.put(MATCH_TAG_ID, MATCH_TAG);
        ID_TO_S.put(STARTS_WITH_ID, STARTS_WITH);
        ID_TO_S.put(REGEX_ID, REGEX);

        TYPE_TO_S.put(AndFilter.class, AND_ID);
        TYPE_TO_S.put(OrFilter.class, OR_ID);
        TYPE_TO_S.put(MatchKeyFilter.class, MATCH_KEY_ID);
        TYPE_TO_S.put(MatchTagFilter.class, MATCH_TAG_ID);
        TYPE_TO_S.put(StartsWithFilter.class, STARTS_WITH_ID);
        TYPE_TO_S.put(RegexFilter.class, REGEX_ID);
    }

    @Override
    public ByteBuffer toByteBuffer(Filter obj) {
        if (obj == null) {
            return ZERO_BUFFER;
        }

        final Composite c = new Composite();

        final Integer typeId = TYPE_TO_S.get(obj.getClass());

        if (typeId == null)
            throw new RuntimeException("No serializer for type "
                    + obj.getClass());

        @SuppressWarnings("unchecked")
        final FilterSerialization<Filter> serializer = (FilterSerialization<Filter>) ID_TO_S
                .get(typeId);

        if (serializer == null)
            throw new RuntimeException("No serializer for type "
                    + obj.getClass());

        c.addComponent(CacheKey.VERSION, integerSerializer);
        c.addComponent(typeId, integerSerializer);

        serializer.serialize(c, obj);

        return c.serialize();
    }

    @Override
    public Filter fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() == 0)
            return null;

        final Composite c = Composite.fromByteBuffer(byteBuffer);

        final int version = c.get(0, integerSerializer);

        if (version != CacheKey.VERSION)
            return null;

        final int typeId = c.get(1, integerSerializer);

        final FilterSerialization<? extends Filter> serializer = ID_TO_S
                .get(typeId);

        if (serializer == null)
            throw new RuntimeException("No serializer for type id " + typeId);

        return serializer.deserialize(c);
    }
}