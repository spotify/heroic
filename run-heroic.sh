#!/bin/sh
exec java $JVM_DEFAULT_ARGS $JVM_ARGS -jar /usr/share/heroic/heroic.jar "$@"
