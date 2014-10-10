package com.spotify.heroic.aggregation.simple;

import javax.inject.Inject;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.HeroicEntryPoint;
import com.spotify.heroic.aggregation.AggregationSerializer;
import com.spotify.heroic.aggregation.AggregationSerializer.Serializer;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.SamplingSerializer;

public class EntryPoint implements HeroicEntryPoint {
    @Inject
    private SamplingSerializer resolutionSerializer;

    @Inject
    private HeroicContext heroicContext;

    private static final short SUM = 0x0001;
    private static final short AVERAGE = 0x0002;
    private static final short MAX = 0x0011;
    private static final short MIN = 0x0012;

    @Override
    public void setup() {
        heroicContext.registerAggregation(SumAggregation.class, SumAggregationQuery.class, SUM,
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

        heroicContext.registerAggregation(AverageAggregation.class, AverageAggregationQuery.class, AVERAGE,
                new Serializer<AverageAggregation>() {
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

        heroicContext.registerAggregation(MinAggregation.class, MinAggregationQuery.class, MIN,
                new AggregationSerializer.Serializer<MinAggregation>() {
                    @Override
                    public MinAggregation deserialize(Composite composite) {
                        final Sampling sampling = composite.get(0, resolutionSerializer);
                        return new MinAggregation(sampling);
                    }

                    @Override
                    public void serialize(Composite composite, MinAggregation value) {
                        composite.addComponent(value.getSampling(), resolutionSerializer);
                    }
                });

        heroicContext.registerAggregation(MaxAggregation.class, MaxAggregationQuery.class, MAX,
                new AggregationSerializer.Serializer<MaxAggregation>() {
                    @Override
                    public MaxAggregation deserialize(Composite composite) {
                        final Sampling sampling = composite.get(0, resolutionSerializer);
                        return new MaxAggregation(sampling);
                    }

                    @Override
                    public void serialize(Composite composite, MaxAggregation value) {
                        composite.addComponent(value.getSampling(), resolutionSerializer);
                    }
                });
    }
}
