package com.spotify.heroic.aggregation;

import java.io.IOException;
import java.util.List;

import lombok.RequiredArgsConstructor;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;

@RequiredArgsConstructor
public abstract class GroupingAggregationSerializer<T extends GroupingAggregation> implements Serializer<T> {
    private final Serializer<List<String>> list;
    private final Serializer<Aggregation> aggregation;

    @Override
    public void serialize(SerialWriter buffer, T value) throws IOException {
        list.serialize(buffer, value.getOf());
        aggregation.serialize(buffer, value.getEach());
    }

    @Override
    public T deserialize(SerialReader buffer) throws IOException {
        final List<String> of = list.deserialize(buffer);
        final Aggregation each = aggregation.deserialize(buffer);
        return build(of, each);
    }

    protected abstract T build(List<String> of, Aggregation each);
}