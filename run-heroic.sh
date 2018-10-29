#!/bin/sh
exec /usr/bin/java $JVM_DEFAULT_ARGS $JVM_ARGS -cp /usr/share/heroic/heroic.jar com.spotify.heroic.HeroicService "$@"
