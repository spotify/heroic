# Bigtable

This contains shading of `com.google.cloud.bigtable:bigtable-client-core`.

It also includes a transitive dependency to `netty-tcnative-boringssl-static`
to make it more opinionated. This does mean that this module will be
platform-specific to where it is being built.

See [pom](pom.xml) for details.

## Relocated Dependencies

```
io.grpc               -> com.spotify.heroic.bigtable.grpc
io.netty              -> com.spotify.heroic.bigtable.netty
org.apache.commons    -> com.spotify.heroic.bigtable.commons
com.fasterxml.jackson -> com.spotify.heroic.bigtable.jackson
```
