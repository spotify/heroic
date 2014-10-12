package com.spotify.heroic.aggregation.simple;

import javax.inject.Inject;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.DoubleSerializer;
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

    private static final DoubleSerializer doubleSerializer = DoubleSerializer.get();

    private static final short SUM = 0x0001;
    private static final short AVERAGE = 0x0002;
    private static final short MAX = 0x0011;
    private static final short MIN = 0x0012;
    private static final short STDDEV = 0x0021;
    private static final short QUANTILE = 0x0022;

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

        heroicContext.registerAggregation(StdDevAggregation.class, StdDevAggregationQuery.class, STDDEV,
                new AggregationSerializer.Serializer<StdDevAggregation>() {
                    @Override
                    public StdDevAggregation deserialize(Composite composite) {
                        final Sampling sampling = composite.get(0, resolutionSerializer);
                        return new StdDevAggregation(sampling);
                    }

                    @Override
                    public void serialize(Composite composite, StdDevAggregation value) {
                        composite.addComponent(value.getSampling(), resolutionSerializer);
                    }
                });

        heroicContext.registerAggregation(QuantileAggregation.class, QuantileAggregationQuery.class, QUANTILE,
                new AggregationSerializer.Serializer<QuantileAggregation>() {
                    @Override
                    public QuantileAggregation deserialize(Composite composite) {
                        final Sampling sampling = composite.get(0, resolutionSerializer);
                        final Double q = composite.get(1, doubleSerializer);
                        return new QuantileAggregation(sampling, q);
                    }

                    @Override
                    public void serialize(Composite composite, QuantileAggregation value) {
                        composite.addComponent(value.getSampling(), resolutionSerializer);
                        composite.addComponent(value.getQ(), doubleSerializer);
                    }
                });
    }
}
