package com.spotify.heroic;

public class JvmClusterQueryIT extends AbstractClusterQueryIT {
    @Override
    protected String protocol() {
        return "jvm";
    }
}
