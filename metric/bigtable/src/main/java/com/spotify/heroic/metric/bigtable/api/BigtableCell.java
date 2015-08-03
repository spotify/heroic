package com.spotify.heroic.metric.bigtable.api;

import lombok.Data;

import com.google.protobuf.ByteString;

@Data
public class BigtableCell {
    final ByteString qualifier;
    final ByteString value;
}
