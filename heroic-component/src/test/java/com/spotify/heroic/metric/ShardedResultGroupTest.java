package com.spotify.heroic.metric;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;

public class ShardedResultGroupTest {
    @Test
    public void testHashGroup() {
        final ShardedResultGroup g1 =
            new ShardedResultGroup(ImmutableMap.of("aa", "bb"), ImmutableMap.of("cc", "dd"),
                ImmutableSet.of(), MetricCollection.empty(), 0L);

        final ShardedResultGroup g2 =
            new ShardedResultGroup(ImmutableMap.of("", ""), ImmutableMap.of("aabb", "ccdd"),
                ImmutableSet.of(), MetricCollection.empty(), 0L);

        assertNotEquals(g1.hashGroup(), g2.hashGroup());
    }
}
