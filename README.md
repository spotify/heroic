# Heroic Documentation Site

This is the code for the official Heroic documentation site hosted at
https://spotify.github.io/heroic/

## Developing

This site is built using [lingon](https://github.com/spotify/lingon), assuming
you have [npm](https://www.npmjs.com/) installed you can run:

```bash
$ npm install
$ node_modules/.bin/bower install
```

Then you can start lingon using:

```bash
$ ./lingon.js
```

Now you should be able to navigate to http://localhost:5678, any edits to the
source will be visible in your browser.

## Publishing

Publishing is done with `tools/publish`, you must setup a remote called
`public` for this to work.

```bash
$ git remote add public git@github.com:spotify/heroic
```

Now you can publish using:

```bash
$ tools/publish
```

Note: this modifies source/index.html in-place, you might want to revert this
using `git checkout source/index.html` after publishing.
