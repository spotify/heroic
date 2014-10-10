package com.spotify.heroic.aggregation;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.SamplingSerializer;

/**
 * Serializer for Aggregation Groups.
 * 
 * @author udoprog
 */
public class AggregationGroupSerializer extends AbstractSerializer<AggregationGroup> {
    private static final IntegerSerializer integerSerializer = IntegerSerializer.get();
    private static final SamplingSerializer samplingSerializer = SamplingSerializer.get();

    @Inject
    private AggregationSerializer aggregationSerializer;

    @Override
    public ByteBuffer toByteBuffer(AggregationGroup obj) {
        final Composite composite = new Composite();
        final List<Aggregation> aggregations = obj.getAggregations();

        composite.addComponent(aggregations.size(), integerSerializer);
        composite.addComponent(obj.getSampling(), samplingSerializer);

        for (Aggregation aggregation : aggregations) {
            composite.addComponent(aggregation, aggregationSerializer);
        }

        return composite.serialize();
    }

    @Override
    public AggregationGroup fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);
        final int size = composite.get(0, integerSerializer);
        final Sampling sampling = composite.get(1, samplingSerializer);

        final List<Aggregation> aggregations = new ArrayList<Aggregation>(size);

        for (int i = 0; i < size; i++) {
            aggregations.add(composite.get(2 + i, aggregationSerializer));
        }

        return new AggregationGroup(aggregations, sampling);
    }
}