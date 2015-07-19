package com.spotify.heroic.aggregation;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationState;
import com.spotify.heroic.aggregation.GroupingAggregation;
import com.spotify.heroic.model.DateRange;

@RunWith(MockitoJUnitRunner.class)
public class GroupingAggregationTest {
    @Mock
    DateRange range;

    @Mock
    Map<String, String> key1;

    @Mock
    Map<String, String> key2;

    @Mock
    AggregationState state1;

    @Mock
    AggregationState state2;

    @Before
    public void setup() {
        doReturn(key1).when(state1).getKey();
        doReturn(key2).when(state2).getKey();
    }

    @Test
    public void testSession() {
        final Aggregation each = mock(Aggregation.class);

        final GroupingAggregation a = spy(new GroupingAggregation(ImmutableList.of("group"), each) {
            @Override
            protected Map<String, String> key(Map<String, String> input) {
                return null;
            }
        });

        doReturn(key1).when(a).key(key1);
        doReturn(key2).when(a).key(key2);

        a.map(ImmutableList.of(state1, state2));
    }
}