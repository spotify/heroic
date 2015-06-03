package com.spotify.heroic.aggregation;

import eu.toolchain.serializer.Serializer;

/**
 * Serializes aggregation configurations.
 *
 * Each aggregation configuration is packed into a Composite which has the type of the aggregation as a prefixed short.
 *
 * @author udoprog
 */
public interface AggregationSerializer extends Serializer<Aggregation> {
    public <T extends AggregationQuery<?>> void registerQuery(String id, Class<T> queryType);

    public <T extends Aggregation> void register(String id, Class<T> clazz, Serializer<T> serializer,
            AggregationBuilder<T> builder);
}