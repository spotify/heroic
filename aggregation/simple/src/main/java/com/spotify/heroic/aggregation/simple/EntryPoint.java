package com.spotify.heroic.aggregation.simple;

import javax.inject.Inject;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.HeroicEntryPoint;
import com.spotify.heroic.JSONContext;
import com.spotify.heroic.aggregation.AggregationSerializer;
import com.spotify.heroic.aggregation.AggregationSerializer.Serializer;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.SamplingSerializer;

public class EntryPoint implements HeroicEntryPoint {
    @Inject
    private AggregationSerializer aggregationSerializer;

    @Inject
    private SamplingSerializer resolutionSerializer;

    @Inject
    private JSONContext jsonContext;

    private static final short SUM_AGGREGATION = 0x0001;
    private static final short AVERAGE_AGGREGATION = 0x0002;

    @Override
    public void setup() {
        jsonContext.registerType("sum", SumAggregation.class);

        aggregationSerializer.register(SumAggregation.class, SUM_AGGREGATION,
                new AggregationSerializer.Serializer<SumAggregation>() {
                    @Override
                    public SumAggregation deserialize(Composite composite) {
                        final Sampling sampling = composite.get(0, resolutionSerializer);
                        return new SumAggregation(sampling);
                    }

                    @Override
                    public void serialize(Composite composite, SumAggregation value) {
                        composite.addComponent(value.getSampling(), resolutionSerializer);
                    }
                });

        jsonContext.registerType("average", AverageAggregation.class);

        aggregationSerializer.register(AverageAggregation.class, AVERAGE_AGGREGATION, new Serializer<AverageAggregation>() {
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
}
