package com.spotify.heroic.aggregationcache.cassandra.model;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.CompositeSerializer;
import com.netflix.astyanax.serializers.ShortSerializer;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AverageAggregation;
import com.spotify.heroic.aggregation.SumAggregation;
import com.spotify.heroic.model.Sampling;

/**
 * Serializes aggregation configurations.
 * 
 * Each aggregation configuration is packed into a Composite which has the type of the aggregation as a prefixed short.
 * 
 * @author udoprog
 */
class AggregationSerializer extends AbstractSerializer<Aggregation> {
    public interface Serializer<T> {
        void serialize(Composite composite, T value);

        T deserialize(Composite composite);
    }

    private static final SamplingSerializer resolutionSerializer = SamplingSerializer.get();
    private static final ShortSerializer shortSerializer = ShortSerializer.get();
    private static final CompositeSerializer compositeSerializer = CompositeSerializer.get();

    private static final Map<Class<? extends Aggregation>, Short> T_TO_ID = new HashMap<Class<? extends Aggregation>, Short>();
    private static final Map<Short, Serializer<? extends Aggregation>> SERIALIZERS = new HashMap<Short, Serializer<? extends Aggregation>>();

    private static final short SUM_AGGREGATION = 0x0001;
    private static final short AVERAGE_AGGREGATION = 0x0002;

    /**
     * Sets up all static mappings and assert that they are unique.
     */
    static {
        T_TO_ID.put(SumAggregation.class, SUM_AGGREGATION);
        T_TO_ID.put(AverageAggregation.class, AVERAGE_AGGREGATION);

        SERIALIZERS.put(SUM_AGGREGATION, new Serializer<SumAggregation>() {
            @Override
            public void serialize(Composite composite, SumAggregation value) {
                composite.addComponent(value.getSampling(), resolutionSerializer);
            }

            @Override
            public SumAggregation deserialize(Composite composite) {
                final Sampling sampling = composite.get(0, resolutionSerializer);
                return new SumAggregation(sampling);
            }
        });

        SERIALIZERS.put(AVERAGE_AGGREGATION, new Serializer<AverageAggregation>() {
            @Override
            public void serialize(Composite composite, AverageAggregation value) {
                composite.addComponent(value.getSampling(), resolutionSerializer);
            }

            @Override
            public AverageAggregation deserialize(Composite composite) {
                final Sampling sampling = composite.get(0, resolutionSerializer);
                return new AverageAggregation(sampling);
            }
        });
    }

    @Override
    public ByteBuffer toByteBuffer(Aggregation obj) {
        final Composite composite = new Composite();
        final Short typeId = T_TO_ID.get(obj.getClass());

        if (typeId == null) {
            throw new RuntimeException("Type is not a serializable aggregate: " + obj.getClass());
        }

        @SuppressWarnings("unchecked")
        final Serializer<Aggregation> serializer = (Serializer<Aggregation>) SERIALIZERS.get(typeId);

        final Composite aggregation = new Composite();
        serializer.serialize(aggregation, obj);

        composite.addComponent(typeId, shortSerializer);
        composite.addComponent(aggregation, compositeSerializer);

        return composite.serialize();
    }

    @Override
    public Aggregation fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);
        final Short typeId = composite.get(0, shortSerializer);
        final Composite aggregation = composite.get(1, compositeSerializer);

        @SuppressWarnings("unchecked")
        final Serializer<Aggregation> serializer = (Serializer<Aggregation>) SERIALIZERS.get(typeId);

        return serializer.deserialize(aggregation);
    }

    private static final AggregationSerializer instance = new AggregationSerializer();

    public static AggregationSerializer get() {
        return instance;
    }
}