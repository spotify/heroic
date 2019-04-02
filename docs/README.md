# Heroic Documentation Development

Documention is served up with GitHub pages at [https://spotify.github.io/heroic/](https://spotify.github.io/heroic/). GitHub pages uses [Jekyll](https://jekyllrb.com/docs/github-pages) to build and serve websites. Jekyll has a CLI tool that can mimic the GitHub pages setup locally.

## Installation

Make sure you have ruby and bundle installed, then from the `docs` directory run `bundle install`.

## Serving

Run:

`bundle exec jekyll serve`

This will compile the site and serve it locally. Any changes will be incrementally re-compiled while the serve command is running. The local documentation can be accessed at [http://127.0.0.1:4000/heroic/](http://127.0.0.1:4000/heroic/).
