#!/bin/bash
MVN=mvn
# quiet
MVN="${MVN} -q"
# release should not be running tests
MVN="${MVN} -D maven.test.skip=true"
# use scoped home directory to avoid contaminating build system
MVN="${MVN} -D user.home=${MAVEN_HOME}"

if [[ -f debian/settings.xml ]]; then
    # use in-project settings.xml
    MVN="${MVN} -s debian/settings.xml"
fi

exec $MVN "$@"
