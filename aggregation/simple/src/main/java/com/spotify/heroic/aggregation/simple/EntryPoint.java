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
    private HeroicContext ctx;

    private static final DoubleSerializer doubleSerializer = DoubleSerializer.get();

    private static final short SUM = 0x0001;
    private static final short AVERAGE = 0x0002;
    private static final short MAX = 0x0011;
    private static final short MIN = 0x0012;
    private static final short STDDEV = 0x0021;
    private static final short QUANTILE = 0x0022;

    @Override
    public void setup() {
        ctx.aggregation(SUM, SumAggregation.class, SumAggregationQuery.class,
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

        ctx.aggregation(AVERAGE, AverageAggregation.class, AverageAggregationQuery.class,
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

        ctx.aggregation(MIN, MinAggregation.class, MinAggregationQuery.class,
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

        ctx.aggregation(MAX, MaxAggregation.class, MaxAggregationQuery.class,
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

        ctx.aggregation(STDDEV, StdDevAggregation.class, StdDevAggregationQuery.class,
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

        ctx.aggregation(QUANTILE, QuantileAggregation.class, QuantileAggregationQuery.class,
                new AggregationSerializer.Serializer<QuantileAggregation>() {
                    @Override
                    public QuantileAggregation deserialize(Composite composite) {
                        final Sampling sampling = composite.get(0, resolutionSerializer);
                        final Double q = composite.get(1, doubleSerializer);
                        final Double error = composite.get(2, doubleSerializer);
                        return new QuantileAggregation(sampling, q, error);
                    }

                    @Override
                    public void serialize(Composite composite, QuantileAggregation value) {
                        composite.addComponent(value.getSampling(), resolutionSerializer);
                        composite.addComponent(value.getQ(), doubleSerializer);
                        composite.addComponent(value.getError(), doubleSerializer);
                    }
                });
    }
}
