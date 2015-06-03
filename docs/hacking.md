# Hacking on Heroic

This project requires the following to work with;

* Java 7+
* A Java Development Environment with...
 * support for [lombok](https://projectlombok.org/).
 * support for Maven.

## Eclipse

This project includes [an eclipse formatter](/eclipse/) which you
should use in your environment (Preferences &rarr; Java &rarr; Code Style
&rarr; Formatter).

You should also do the following in your `Preferences`.

* Enable `Save Actions` to automatically format all source when saving
  (`Java` &rarr; `Save Actions`).
* Show whitespace characters, and make sure tabs, and ideographic spaces are
  visible. Hide leading, and enclosed spaces. Hide newline characters.
  (`General` &rarr; `Editors` &rarr; `Text Editors`).
* **testing**
  Add to favorites
  (`Java` &rarr; `Editor` &rarr; `Content Assist` &rarr; `Favorites`).
  * `org.junit.Assert.*`
  * `org.mockito.Mockito.*`
  * `org.mockito.Matchers.*`

# Tests

Always run tests before publishing a pull request.

```
mvn clean test
```

Make sure that critical (hot) code paths are covered by unit tests, we strongly
favor Mockito.
To simplify testing, components should be isolated through interfaces.
