//= include '../bower_components/angular/angular.js'
//= include '../bower_components/angular-ui-bootstrap-bower/ui-bootstrap-tpls.js'
//= include '../bower_components/angular-ui-router/release/angular-ui-router.js'

//= include 'templates.js'
//= include '_js/prism.js'
//= include '_js/api.js'
//= include '_pages/*.js'

(function() {
  var m = angular.module('hdoc', [
    'ui.router',
    'ui.bootstrap',
    'hdoc.index',
    'hdoc.docs',
    'hdoc.tutorial',
    'hdoc.api'
  ]);

  var LINESEP = /[\n]/;

  function stripText($element, removeIndent) {
    removeIndent = removeIndent || false;

    var text = $element.text();
    var prefix = null;

    if (removeIndent) {
      var previous = $element[0].previousSibling;

      // if previous node is text.
      if (previous !== null && previous.nodeType === 3) {
        var parts = previous.nodeValue.split(LINESEP);
        prefix = parts[parts.length - 1];
      }
    }

    var lines = text.split(LINESEP);

    var result;

    if (prefix === null) {
      result = lines;
    } else {
      result = [];

      for (var i = 0; i < lines.length; i++) {
        var line = lines[i];

        if (line.length < prefix.length) {
          result.push(line);
          continue;
        }

        result.push(line.substring(prefix.length, line.length));
      }
    }

    var index = 0;

    while (result[index] === "") {
      index++;
    }

    return result.slice(index).join('\n');
  }

  function HeroicDocumentationCtrl() {
  }

  m.controller('HeroicDocumentationCtrl', HeroicDocumentationCtrl);

  m.config(function($stateProvider, $urlRouterProvider, $locationProvider, githubProvider, $uiViewScrollProvider) {
    $locationProvider.html5Mode(false).hashPrefix('!');
    $urlRouterProvider.otherwise("/index");
    githubProvider.setUrl('https://github.com/spotify/heroic');
  });

  m.provider('github', function() {
    var githubUrl = null;
    var githubBranch = 'master';

    this.setUrl = function(url) {
      githubUrl = url;
    };

    this.setBranch = function(branch) {
      githubBranch = branch;
    };

    this.$get = function() {
      return {
        url: githubUrl,
        relativeUrl: function(path) {
          return githubUrl + '/' + path;
        },
        blobUrl: function(path) {
          return githubUrl + '/blob/' + githubBranch + '/' + path;
        }
      };
    };
  });

  m.directive('code', function() {
    return {
      restrict: 'E',
      link: function($scope, $element, $attr) {
        if (!$attr.language)
          return;

        $element.text(stripText($element));
        $element.addClass('language-' + $attr.language);
        Prism.highlightElement($element[0]);
      }
    };
  });

  m.directive('gitHrefPackage', function(github) {
    return {
      restrict: 'A',
      link: function($scope, $element, $attr) {
        var href = $attr.gitHrefPackage;
        var colon = href.indexOf(':');

        var component, path;

        if (colon == -1) {
          component = href;
          path = $element.text();
        } else {
          component = href.substring(0, colon);
          path = href.substring(colon + 1, href.length);
        }

        path = path.replace(/\./g, '/');
        var newHref = component + '/src/main/java/' + path;
        $element.attr('href', github.blobUrl(newHref));
      }
    };
  });

  m.directive('gitHrefJava', function(github) {
    return {
      restrict: 'A',
      link: function($scope, $element, $attr) {
        var href = $attr.gitHrefJava;
        var colon = href.indexOf(':');

        var component, path;

        if (colon == -1) {
          component = href;
          path = $element.text();
        } else {
          component = href.substring(0, colon);
          path = href.substring(colon + 1, href.length);
        }

        path = path.replace(/\./g, '/') + '.java';
        var newHref = component + '/src/main/java/' + path;
        $element.attr('href', github.blobUrl(newHref));
      }
    };
  });

  m.directive('gitHref', function(github) {
    return {
      restrict: 'A',
      link: function($scope, $element, $attr) {
        $element.attr('href', github.relativeUrl($attr.gitHref));
      }
    };
  });

  m.directive('id', function($state) {
    return {
      restrict: 'A',
      link: function($scope, $element, $attr) {
        var $glyph = angular.element('<span class="glyphicon glyphicon-link">');

        var $ln = angular.element('<a class="link-to">');
        $ln.append($glyph);

        $element.append($ln);

        $scope.$watch(function() {
          return $attr.id;
        }, function(newId) {
          $ln.attr('href', $state.href('.', {'#': newId}));
        });
      }
    };
  });

  /**
   * Helper directive for creating indented code blocks.
   *
   * Will setup, and compile a code block containing <pre><code language=...>.
   *
   * @param content Specify dynamic content of the code block.
   * @param language Specify the language of the code block.
   */
  m.directive('codeblock', function($compile) {
    return {
      link: function($scope, $element, $attr) {
        var children = $element.children();
        var pre = angular.element('<pre>');
        var code = angular.element('<code>');

        if (!!$attr.language)
          code.attr('language', $attr.language);

        pre.append(code);

        if (!!$attr.content) {
          $element.replaceWith(pre);

          $scope.$watch($attr.content, function(content) {
            content = content || '';
            code.text(content);
            $compile(pre)($scope);
          });

          return;
        }

        var text = stripText($element, true);

        $element.replaceWith(pre);
        code.text(text);
        $compile(pre)($scope);
      }
    };
  });
})();
