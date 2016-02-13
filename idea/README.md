# Hacking with IDEA

Import the project as a Maven project.

Make sure to install the [Lombok plugin](https://plugins.jetbrains.com/plugin/6317)
(File &rarr; Settings &rarr; Plugins &rarr; Browse Repositories...).

There is a [code style available](code-style.xml).
Import this into your IDE, and make sure to use it (File &rarr; Settings &rarr;
Editor &rarr; Code Style &rarr; Manage...).

Keep in mind that a `mvn clean ...` will delete generated-sources, which means
IDEA will loose track of a bunch of generated files.
If this happens, rebuild the project using `CTRL + F9`.
