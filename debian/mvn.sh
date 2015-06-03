#!/bin/bash
MVN=mvn
# quiet
MVN+=-q
# release should not be running tests
MVN+=-D maven.test.skip=true
# use scoped home directory to avoid contaminating build system
MVN+=-D user.home=$(MAVEN_HOME)

if [[ -f debian/settings.xml ]]; then
    # use in-project settings.xml
    MVN+=-s debian/settings.xml
fi

exec $MVN "$@"
