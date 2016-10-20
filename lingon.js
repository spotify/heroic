#! /usr/bin/env node

var lingon            = require('lingon');
var html2js           = require('lingon-ng-html2js');
var autoprefixer      = require('gulp-autoprefixer');
var ngAnnotate        = require('gulp-ng-annotate');

lingon.config.sourcePath = 'source';
lingon.config.buildPath = 'output';
lingon.config.server.catchAll = 'index.html';

lingon.preProcessors.push('ngt', function(context, globals) {
  return html2js({base: 'source', stripPrefix: 'source/'});
});

lingon.postProcessors.push('js', /^((?!spec\.).)*$/, function(params) {
  var processors = [];

  // minification safe angular array syntax
  if (lingon.task == 'build') {
    processors.push(
      ngAnnotate()
    );
  }

  return processors;
});

lingon.postProcessors.push('less', function() {
  return autoprefixer();
});
