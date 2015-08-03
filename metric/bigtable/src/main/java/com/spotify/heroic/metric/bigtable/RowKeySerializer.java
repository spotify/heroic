package com.spotify.heroic.metric.bigtable;

import java.io.IOException;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.model.Series;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;

@RequiredArgsConstructor
public class RowKeySerializer implements Serializer<RowKey> {
    private final Serializer<Series> series;
    private final Serializer<Long> longNumber;

    @Override
    public void serialize(SerialWriter buffer, RowKey value) throws IOException {
        series.serialize(buffer, value.getSeries());
        longNumber.serialize(buffer, value.getBase());
    }

    @Override
    public RowKey deserialize(SerialReader buffer) throws IOException {
        final Series series = this.series.deserialize(buffer);
        final long base = this.longNumber.deserialize(buffer);
        return new RowKey(series, base);
    }
}