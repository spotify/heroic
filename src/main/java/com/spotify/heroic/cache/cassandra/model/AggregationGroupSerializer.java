package com.spotify.heroic.cache.cassandra.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationGroup;

/**
 * Serializer for AggregationGroup's.
 *
 * @author udoprog
 */
class AggregationGroupSerializer extends AbstractSerializer<AggregationGroup> {
    private static final IntegerSerializer integerSerializer = IntegerSerializer.get();
    private static final AggregationSerializer aggregationSerializer = AggregationSerializer.get();

    private static final AggregationGroupSerializer instance = new AggregationGroupSerializer();

    public static AggregationGroupSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(AggregationGroup aggregationGroup) {
        final Composite composite = new Composite();
        final List<Aggregation> aggregations = aggregationGroup.getAggregations();

        composite.addComponent(aggregations.size(), integerSerializer);

        for (Aggregation aggregation : aggregations) {
            composite.addComponent(aggregation, aggregationSerializer);
        }

        return composite.serialize();
    }

    @Override
    public AggregationGroup fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);
        final int size = composite.get(0, integerSerializer);

        final List<Aggregation> aggregations = new ArrayList<Aggregation>(size);

        for (int i = 0; i < size; i++) {
            aggregations.add(composite.get(1 + i, aggregationSerializer));
        }

        return new AggregationGroup(aggregations);
    }
}