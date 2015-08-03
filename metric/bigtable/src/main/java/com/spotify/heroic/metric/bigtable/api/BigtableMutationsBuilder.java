package com.spotify.heroic.metric.bigtable.api;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

import com.google.bigtable.v1.Mutation;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

@Data
public class BigtableMutationsBuilder {
    final List<Mutation> mutations = new ArrayList<>();

    public BigtableMutationsBuilder setCell(String family, ByteString columnQualifier, ByteString value) {
        Mutation.SetCell.Builder setCell = Mutation.SetCell.newBuilder().setFamilyName(family)
                .setColumnQualifier(columnQualifier).setValue(value);

        mutations.add(Mutation.newBuilder().setSetCell(setCell).build());
        return this;
    }

    public BigtableMutations build() {
        return new BigtableMutations(ImmutableList.copyOf(mutations));
    }
}