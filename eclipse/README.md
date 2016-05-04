# Using Eclipse

_Note_: Due to [a bug in annotation processing](https://bugs.eclipse.org/bugs/show_bug.cgi?id=382590)
that interacts badly with Dagger 2, using Eclipse is _not recommended_.
Consider using [IDEA CE](../idea) instead.

You must use at least Eclipse 4.5.0, this is due to issues with [annotation
processing](https://bugs.eclipse.org/bugs/show_bug.cgi?id=300408) in earlier
versions.

The following plugins are required:

* lombok (https://projectlombok.org/download.html)
* m2e-apt (install through Marketplace)
  * Enable automatic configuration of JDT APT in
    **Preferences &rarr; Maven &rarr; Annotation Processing**.
* m2e checkstyle connector (will be required on initial import)
  * Configure it to use the provided [checkstyle.xml](/checkstyle.xml) file in
    **Preferences &rarr; Checkstyle**.
    You must set the `${basedir}` property to the path of the checked out
    heroic project.

The project contains a repackaged version of
[bigtable-client-core](http://search.maven.org/#artifactdetails%7Ccom.google.cloud.bigtable%7Cbigtable-client-core%7C0.2.1%7Cjar)
which you should install locally to make it easier for Eclipse to discover:

```bash
$ cd repackaged/bigtable && mvn clean install
```

Import the directory as a Maven project in
**File &rarr; Import &rarr; Maven &rarr; Existing Maven Projects**,
select all discovered modules.

There is a [formatter available](formatter.xml). It is recommended that you
import this into your IDE in
**Preferences &rarr; Java &rarr; Code Style &rarr; Formatter**.
You should also set a save action to format all lines and organize imports when
you save in
**Preferences &rarr; Java &rarr; Editor &rarr; Save Actions**.
