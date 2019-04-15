# Contributing to Heroic

Thanks for taking the time to contribute! 
The following is a set of guidelines for contributing to Heroic. 

## Ground Rules

* Be sure to add a comment to your PR explaining the reason for the change. Please include some context behind a bug or issue you ran in to, or your use case and why you need a particular feature added. 
* Include tests for any large changes. Read here to learn how to run the [tests](https://github.com/spotify/heroic#testing).

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

* Before setting up a pull request, run the comprehensive test suite as specified in
[Testing](#testing).

### Testing

* Before setting up a pull request, run the comprehensive test suite as specified in Testing.

[A Guide to Dagger 2](docs/guide-to-dagger2.md)

[Using IDEA](idea/README.md)

## How to report a bug

### Security Disclosures

If you find a security vulnerability, do NOT open an issue. Email security@spotify.com instead.

### How to File a Bug Report

When filing an issue, make sure to answer these 3 questions:
1. What did you do?
2. What did you expect to see?
3. What did you see instead?

## How to suggest a feature or enhancement

If you find yourself wishing for a feature that doesn't exist in Heroic, you are probably not alone. There are bound to be others out there with similar needs. Open an issue on our issues list on GitHub which describes the feature you would like to see, why you need it, and how it should work.

## Code review process

The core team members look over Pull Requests on a regular basis. 
Someone from the team will review the PR and respond within a week. 
The repo is open to the public for commits. 
 
## Community
 
 
## Code of Conduct

This project adheres to the [Open Code of Conduct](https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md). By participating, you are expected to honor this code.

