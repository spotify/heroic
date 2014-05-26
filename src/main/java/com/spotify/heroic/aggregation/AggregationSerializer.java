package com.spotify.heroic.aggregation;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.CompositeSerializer;
import com.netflix.astyanax.serializers.ShortSerializer;

/**
 * Serializes aggregation configurations.
 * 
 * Each aggregation configuration is packed into a Composite which has the type
 * of the aggregation as a prefixed short.
 * 
 * @author udoprog
 */
public class AggregationSerializer extends AbstractSerializer<Aggregation> {
    private static final Map<Short, Class<? extends Aggregation>> TYPES = new HashMap<Short, Class<? extends Aggregation>>();
    private static final Map<Class<? extends Aggregation>, Short> MAP = new HashMap<Class<? extends Aggregation>, Short>();

    public static final short SUM_AGGREGATION = 0x0001;
    public static final short AVERAGE_AGGREGATION = 0x0002;

    /**
     * Sets up all static mappings and assert that they are unique.
     */
    static {
        TYPES.put(SUM_AGGREGATION, SumAggregation.class);
        TYPES.put(AVERAGE_AGGREGATION, AverageAggregation.class);

        for (Map.Entry<Short, Class<? extends Aggregation>> entry : TYPES
                .entrySet()) {
            MAP.put(entry.getValue(), entry.getKey());
        }
    }

    private static Class<? extends Aggregation> findClass(short type) {
        return TYPES.get(type);
    }

    private static short findType(Class<? extends Aggregation> clazz) {
        return MAP.get(clazz);
    }

    public static final AggregationSerializer instance = new AggregationSerializer();

    public static AggregationSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Aggregation obj) {
        final Composite composite = new Composite();
        final Short type = findType(obj.getClass());

        if (type == null) {
            throw new RuntimeException(
                    "Type is not a serializable aggregate: "
                            + obj.getClass());
        }

        final Composite aggregation = new Composite();
        obj.serialize(aggregation);

        composite.addComponent(type, ShortSerializer.get());
        composite.addComponent(aggregation, CompositeSerializer.get());

        return composite.serialize();
    }

    @Override
    public Aggregation fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);
        final Short type = composite.get(0, ShortSerializer.get());
        final Composite aggregator = composite.get(1, CompositeSerializer.get());
        return buildInstance(aggregator, type);
    }

    private Aggregation buildInstance(final Composite aggregator, short typeId) {
        final Class<? extends Aggregation> type = findClass(typeId);

        if (type == null) {
            throw new RuntimeException("No such aggregation type: " + type);
        }

        final Constructor<? extends Aggregation> constructor;

        try {
            constructor = type.getConstructor(Composite.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(
                    "No Composite constructor found for aggregator " + type, e);
        }

        try {
            return constructor.newInstance(aggregator);
        } catch (ReflectiveOperationException | IllegalArgumentException e) {
            throw new RuntimeException(
                    "Failed to create new aggregation instance", e);
        }
    }
}