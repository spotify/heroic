package com.spotify.heroic.aggregation;

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
 * Each aggregation configuration is packed into a Composite which has the type of the aggregation as a prefixed short.
 * 
 * @author udoprog
 */
public class AggregationSerializer extends AbstractSerializer<Aggregation> {
    public static interface Serializer<T> {
        void serialize(Composite composite, T value);

        T deserialize(Composite composite);
    }

    private static final ShortSerializer shortSerializer = ShortSerializer.get();
    private static final CompositeSerializer compositeSerializer = CompositeSerializer.get();

    private final Map<Class<? extends Aggregation>, Short> types = new HashMap<>();
    private final Map<Short, Serializer<? extends Aggregation>> serializers = new HashMap<>();

    public <T extends Aggregation> void register(Class<T> clazz, short id, Serializer<T> serializer) {
        if (types.put(clazz, id) != null) {
            throw new IllegalArgumentException("A type with the id '" + Short.toString(id) + "' is already registered.");
        }

        serializers.put(id, serializer);
    }

    @Override
    public ByteBuffer toByteBuffer(Aggregation obj) {
        final Composite composite = new Composite();
        final Short typeId = types.get(obj.getClass());

        if (typeId == null) {
            throw new RuntimeException("Type is not a serializable aggregate: " + obj.getClass());
        }

        @SuppressWarnings("unchecked")
        final Serializer<Aggregation> serializer = (Serializer<Aggregation>) serializers.get(typeId);

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
        final Serializer<Aggregation> serializer = (Serializer<Aggregation>) serializers.get(typeId);

        return serializer.deserialize(aggregation);
    }
}