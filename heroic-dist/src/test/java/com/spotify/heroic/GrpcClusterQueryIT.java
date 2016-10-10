package com.spotify.heroic;

public class GrpcClusterQueryIT extends AbstractClusterQueryIT {
    @Override
    protected String protocol() {
        return "grpc";
    }

    @Override
    protected void setupSupport() {
        super.setupSupport();

        // TODO: over GRPC something goes wrong with the distributed cardinality support, or it's an
        // error.
        cardinalitySupport = false;
    }
}
