# Contributing to Heroic

Thanks for taking the time to contribute! 

The following is a set of guidelines for contributing to Heroic. 

## Ground Rules

* Please open an [issue](https://github.com/spotify/heroic/issues) for discussion before submitting any major changes. 
* Be sure to add a title and description to your PR explaining the reason for the change. Please include some context behind a bug or issue you ran in to, or your use case and why you need a particular feature added. If you're unsure about formatting, you can follow [this article](https://medium.com/@steveamaza/how-to-write-a-proper-git-commit-message-e028865e5791) which dives into writing a proper commit message. 
* Include tests for any large changes. Read here to learn how to run the [tests](https://github.com/spotify/heroic#testing).
* Include new or updated documentation in the related PR.
## Your First Contribution

Unsure where to begin contributing to Heroic? You can start by browsing through these [starter issues](https://github.com/spotify/heroic/labels/level%3Astarter).

Working on your first Pull Request? You can learn how from this free series, [How to Contribute to an Open Source Project on GitHub](https://egghead.io/series/how-to-contribute-to-an-open-source-project-on-github).

## Getting started

We use GitHub to manage issues. You can follow along and create issues [here](https://github.com/spotify/heroic/issues). 

### Create your own fork of the code

### Code style

* Make sure you format the code using the provided formatter in [idea](https://github.com/spotify/heroic/blob/master/idea). Even if you disagree with the way it is formatted, consistency is more important. For special cases, see [Bypassing Validation](https://github.com/spotify/heroic#bypassing-validation).

### Commit message conventions

* If possible, limit your changes to one module per commit.
If you add new, or modify existing classes. Keep that change to a single commit while maintaining
backwards compatible behaviour. Deprecate any old APIs as appropriate with `@Deprecated` and
add documentation for how to use the new API.

* The first line of the commit should be formatted with `[module1,module2] my message`.

* `module1` and `module2` are paths to the modules affected with any `heroic-` prefix stripped.
So if your change affects `heroic-core` and `metric/bigtable`, the message should say
`[core,metric/bigtable] did x to y`.

* If more than _3 modules_ are affected by a commit, use `[all]`.
For other cases, adapt to the format of existing commit messages.

### Testing

* Before setting up a pull request, run the comprehensive test suite as specified in
[Testing](#testing).
* PRs with failing tests will not be merged.

[A Guide to Dagger 2](docs/guide-to-dagger2.md)

[Using IDEA](idea/README.md)

## How to report a bug

Please open an [issue under the bug label](https://github.com/spotify/heroic/issues?q=is%3Aopen+is%3Aissue+label%3Atype%3Abug) and we will prioritize it based on the severity.

## How to suggest a feature or enhancement

If you find yourself wishing for a feature that doesn't exist in Heroic, you are probably not alone. Open an issue on our [issues list](https://github.com/spotify/heroic/issues) on GitHub which describes the feature you would like to see, why you need it, and how it should work.
 
## Code of Conduct

This project adheres to the [Open Code of Conduct](https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md). By participating, you are expected to honor this code.

