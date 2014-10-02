package com.spotify.heroic.aggregationcache.cassandra.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.model.Sampling;

/**
 * Serializer for Aggregation Groups.
 * 
 * @author udoprog
 */
class AggregationGroupSerializer extends AbstractSerializer<AggregationGroup> {
    private static final IntegerSerializer INTEGER_SERIALIZER = IntegerSerializer.get();
    private static final AggregationSerializer AGGREGATION_SERIALIZER = AggregationSerializer.get();
    private static final SamplingSerializer SAMPLING_SERIALIZER = SamplingSerializer.get();

    private static final AggregationGroupSerializer instance = new AggregationGroupSerializer();

    public static AggregationGroupSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(AggregationGroup obj) {
        final Composite composite = new Composite();
        final List<Aggregation> aggregations = obj.getAggregations();

        composite.addComponent(aggregations.size(), INTEGER_SERIALIZER);
        composite.addComponent(obj.getSampling(), SAMPLING_SERIALIZER);

        for (Aggregation aggregation : aggregations) {
            composite.addComponent(aggregation, AGGREGATION_SERIALIZER);
        }

        return composite.serialize();
    }

    @Override
    public AggregationGroup fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);
        final int size = composite.get(0, INTEGER_SERIALIZER);
        final Sampling sampling = composite.get(1, SAMPLING_SERIALIZER);

        final List<Aggregation> aggregations = new ArrayList<Aggregation>(size);

        for (int i = 0; i < size; i++) {
            aggregations.add(composite.get(2 + i, AGGREGATION_SERIALIZER));
        }

        return new AggregationGroup(aggregations, sampling);
    }
}