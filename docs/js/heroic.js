Prism.languages.hql = {
  'string': /("(?!:)(\\?[^'"])*?"(?!:)|\b[a-z\.]+\b)/g
};

Prism.languages.insertBefore('hql', 'string', {
  'operator': /(\+|-|!=|=|!\^|\^|!)|(\b(and|or|not|in|with|as|by)\b)/g,
  'keyword': /(\$[a-z]+|\$now|<[a-z]+>|\b[0-9]+(ms|s|m|H|d|w)\b)/g
});

(function() {
  var m = angular.module('hdoc.api', [
    '_js/api-endpoint.ngt',
    '_js/api-response.ngt',
    '_js/api-accept.ngt',
    '_js/api-type.ngt'
  ]);

  function nameToType(name) {
    if (!name)
      throw new Error('name must be defined');

    name = name.replace(/([a-z])([A-Z])/g, '$1-$2').toLowerCase();
    name = name.replace(/[\._]/g, '-');
    return 'type-' + name;
  }

  function pathToId(method, path) {
    var parts = path.split('/');

    var id = [method.toLowerCase()];

    for (var i = 1, l = parts.length; i < l; i++) {
      id.push(parts[i]);
    }

    return id.join('-');
  }

  m.directive('apiEndpoint', function() {
    return {
      scope: {},
      restrict: 'E',
      transclude: true,
      replace: true,
      templateUrl: '_js/api-endpoint.ngt',
      require: 'apiEndpoint',
      link: function($scope, $element, $attr, ctrl) {
        ctrl.path = $attr.path || '/';
        ctrl.method = $attr.method || 'GET';
        $scope.id = pathToId(ctrl.method, ctrl.path);
        $scope.path = ctrl.path;
        $scope.method = ctrl.method;
      },
      controller: ["$scope", function($scope) {
      }]
    };
  });

  m.directive('apiResponse', function() {
    return {
      scope: {status: '@', contentType: '@'},
      restrict: 'E',
      transclude: true,
      replace: true,
      templateUrl: '_js/api-response.ngt',
      require: '^apiEndpoint',
      link: function($scope, $element, $attr, endpoint) {
        $scope.status = $scope.status || '200';
        $scope.contentType = $scope.contentType || 'application/json';
      }
    };
  });

  m.directive('apiTypeId', function() {
    return {
      link: function($scope, $element, $attr) {
        $element.attr('id', nameToType($attr.apiTypeId));
      }
    };
  });

  m.directive('apiType', function() {
    return {
      scope: {},
      restrict: 'E',
      transclude: true,
      templateUrl: '_js/api-type.ngt',
      replace: true,
      compile: function($element, $attr, transclude) {
        return function($scope, $element, $attr) {
          $scope.structural = $attr.kind === 'structural';
          $scope.name = $attr.name || null;
          $scope.id = $scope.name !== null ? nameToType($scope.name) : null;
        };
      },
      controller: ["$scope", function ApiTypeCtrl($scope) {
        $scope.fields = [];

        this.addField = function(field) {
          $scope.fields.push(field);
        };
      }]
    };
  });

  m.directive('apiFieldBind', ["$compile", function($compile) {
    return {
      link: function($scope, $element, $attr) {
        $scope.$watch($attr.apiFieldBind, function(element) {
          if (!element)
            return;

          $element.children().remove();
          $element.append(element);
          $compile(element)($scope);
        });
      }
    };
  }]);

  m.directive('apiField', ["$sce", "$state", function($sce, $state) {
    function compileTypeHref(typeHref) {
      var code = angular.element('<code>');
      var a = angular.element('<a>');
      var glyph = angular.element('<span class="glyphicon glyphicon-link">');
      var smallGlyph = angular.element('<small>');
      smallGlyph.append(glyph);

      var href = $state.href('^.' + nameToType(typeHref));

      a.attr('href', href);
      code.text(typeHref);

      a.append(code);
      a.append(smallGlyph);

      return a;
    }

    function compileTypeArrayHref(typeArrayHref) {
      var code = angular.element('<code>');
      var a = angular.element('<a>');
      var glyph = angular.element('<span class="glyphicon glyphicon-link">');
      var smallGlyph = angular.element('<small>');
      smallGlyph.append(glyph);

      var href = $state.href('^.' + nameToType(typeArrayHref));

      a.attr('href', href);
      code.text("[" + typeArrayHref + ", ...]");

      a.append(code);
      a.append(smallGlyph);

      return a;
    }

    function compileTypeJson(typeJson) {
      var code = angular.element('<code language="json">');

      code.text(typeJson);

      return code;
    }

    function compileType($attr) {
      if (!!$attr.typeHref)
        return compileTypeHref($attr.typeHref);

      if (!!$attr.typeArrayHref)
        return compileTypeArrayHref($attr.typeArrayHref);

      if (!!$attr.typeJson)
        return compileTypeJson($attr.typeJson);

      var noType = angular.element('<em>');
      noType.text('no type');
      return noType;
    }

    return {
      require: '^apiType',
      compile: function($element, $attr, $transclude) {
        return function($scope, $element, $attr, type) {
          var compiledType = compileType($attr);

          var field = {
            name: $attr.name,
            required: $attr.required === 'true' ? true : false,
            type: compiledType,
            purpose: $element.clone().contents()
          };

          $element.remove();
          type.addField(field);
        };
      }
    };
  }]);

  m.directive('apiAccept', ["$sce", function($sce) {
    return {
      restrict: 'E',
      transclude: true,
      replace: true,
      templateUrl: '_js/api-accept.ngt',
      require: '^apiEndpoint',
      link: function($scope, $element, $attr, endpoint) {
        $scope.contentType = $attr.contentType || 'application/json';
        $scope.curl = null;
        $scope.isEmpty = true;
        $scope.curlData = $attr.curlData || '{}';

        var buildCurl = function(contentType, isEmpty) {
          var curl = "$ curl";

          if ((endpoint.method !== 'GET' || !isEmpty) && endpoint.method !== 'POST')
            curl += " -X" + endpoint.method;

          if (!isEmpty)
            curl += " -H \"Content-Type: " + $scope.contentType + "\"";

          curl += " http://localhost:8080" + endpoint.path;

          if (!isEmpty)
            curl += " \\\n  -d '" + $scope.curlData + "'";

          return $sce.trustAsHtml(curl);
        };

        $scope.$watch($attr.empty, function(empty) {
          $scope.isEmpty = !!empty;
          $scope.curl = buildCurl($scope.contentType, $scope.isEmpty);
        });
      }
    };
  }]);
})();

(function() {
  DocumentationCtrl.$inject = ["$scope"];
  var m = angular.module('hdoc.docs', [
    'hdoc.docs.api',
    '_pages/docs.ngt',
    '_pages/docs/getting_started.ngt',
    '_pages/docs/getting_started/installation.ngt',
    '_pages/docs/getting_started/configuration.ngt',
    '_pages/docs/getting_started/compile.ngt',
    '_pages/docs/data_model.ngt',
    '_pages/docs/query_language.ngt',
    '_pages/docs/overview.ngt',
    '_pages/docs/aggregations.ngt',
    '_pages/docs/shell.ngt',
    '_pages/docs/profiles.ngt',
    '_pages/docs/federation.ngt',
    '_pages/docs/federation-tail.ngt',
    '_pages/docs/config.ngt',
    '_pages/docs/config/cluster.ngt',
    '_pages/docs/config/metrics.ngt',
    '_pages/docs/config/metadata.ngt',
    '_pages/docs/config/suggest.ngt',
    '_pages/docs/config/elasticsearch_connection.ngt',
    '_pages/docs/config/shell_server.ngt',
    '_pages/docs/config/consumer.ngt',
    '_pages/docs/config/features.ngt',
    '_pages/docs/config/query_logging.ngt'
  ]);

  function DocumentationCtrl($scope) {
    $scope.endpoints = endpoints;
  }

  m.controller('DocumentationCtrl', DocumentationCtrl);

  m.config(["$stateProvider", function($stateProvider) {
    $stateProvider
      .state('docs', {
        abstract: true,
        url: "/docs",
        templateUrl: "_pages/docs.ngt",
        controller: DocumentationCtrl
      })
      .state('docs.getting_started', {
        abstract: true,
        url: '/getting_started',
        template: '<ui-view></ui-view>'
      })
      .state('docs.getting_started.index', {
        url: '',
        templateUrl: '_pages/docs/getting_started.ngt'
      })
      .state('docs.getting_started.installation', {
        url: '/installation',
        templateUrl: '_pages/docs/getting_started/installation.ngt'
      })
      .state('docs.getting_started.compile', {
        url: '/compile',
        templateUrl: '_pages/docs/getting_started/compile.ngt'
      })
      .state('docs.getting_started.configuration', {
        url: '/configuration',
        templateUrl: '_pages/docs/getting_started/configuration.ngt'
      })
      .state('docs.overview', {
        url: '/overview',
        templateUrl: '_pages/docs/overview.ngt'
      })
      .state('docs.query_language', {
        url: '/query_language',
        templateUrl: '_pages/docs/query_language.ngt'
      })
      .state('docs.data_model', {
        url: '/data_model',
        templateUrl: '_pages/docs/data_model.ngt'
      })
      .state('docs.aggregations', {
        url: '/aggregations',
        templateUrl: '_pages/docs/aggregations.ngt'
      })
      .state('docs.shell', {
        url: '/shell',
        templateUrl: '_pages/docs/shell.ngt'
      })
      .state('docs.federation', {
        url: '/federation',
        templateUrl: '_pages/docs/federation.ngt'
      })
      .state('docs.profiles', {
        url: '/profiles',
        templateUrl: '_pages/docs/profiles.ngt'
      })
      .state('docs.config.index', {
        url: '',
        templateUrl: '_pages/docs/config.ngt'
      })
      .state('docs.config', {
        abstract: true,
        url: '/config',
        template: '<ui-view></ui-view>'
      })
      .state('docs.config.cluster', {
        url: '/cluster',
        templateUrl: '_pages/docs/config/cluster.ngt'
      })
      .state('docs.config.metrics', {
        url: '/metrics',
        templateUrl: '_pages/docs/config/metrics.ngt'
      })
      .state('docs.config.metadata', {
        url: '/metadata',
        templateUrl: '_pages/docs/config/metadata.ngt'
      })
      .state('docs.config.suggest', {
        url: '/suggest',
        templateUrl: '_pages/docs/config/suggest.ngt'
      })
      .state('docs.config.elasticsearch_connection', {
        url: '/elasticsearch_connection',
        templateUrl: '_pages/docs/config/elasticsearch_connection.ngt'
      })
      .state('docs.config.shell_server', {
        url: '/shell_server',
        templateUrl: '_pages/docs/config/shell_server.ngt'
      })
      .state('docs.config.consumer', {
        url: '/consumer',
        templateUrl: '_pages/docs/config/consumer.ngt'
      })
      .state('docs.config.features', {
        url: '/features',
        templateUrl: '_pages/docs/config/features.ngt'
      })
      .state('docs.config.query_logging', {
        url: '/query_logging',
        templateUrl: '_pages/docs/config/query_logging.ngt'
      });
  }]);
})();

(function() {
  var m = angular.module('hdoc.index', [
    '_pages/index.ngt'
  ]);

  m.config(["$stateProvider", function($stateProvider) {
    $stateProvider
      .state('index', {
        url: "/index",
        templateUrl: "_pages/index.ngt"
      });
  }]);
})();

(function() {
  var m = angular.module('hdoc.tutorial', [
    '_pages/tutorial/index.ngt',
    '_pages/tutorial/kafka_consumer.ngt',
  ]);

  m.config(["$stateProvider", function($stateProvider) {
    $stateProvider
      .state('tutorial', {
        abstract: true,
        url: '/tutorial',
        template: '<ui-view></ui-view>'
      })
      .state('tutorial.index', {
        url: "/index",
        templateUrl: "_pages/tutorial/index.ngt"
      })
      .state('tutorial.kafka_consumer', {
        url: "/kafka_consumer",
        templateUrl: "_pages/tutorial/kafka_consumer.ngt"
      });
  }]);
})();

(function() {
  DocumentationCtrl.$inject = ["$scope"];
  var endpointUrls = endpoints.map(function(e) {
    return '_pages/docs/api/' + e.sref + '.ngt';
  });

  var typeUrls = types.map(function(t) {
    return '_pages/docs/api/type-' + t.id + '.ngt';
  });

  var m = angular.module('hdoc.docs.api', [
    '_pages/docs/api.ngt',
    '_pages/docs/api/accept-metadata-query-body.ngt',
    '_pages/docs/api/accept-series.ngt',
  ].concat(endpointUrls).concat(typeUrls));

  function DocumentationCtrl($scope) {
    $scope.endpoints = endpoints;
    $scope.types = types;

    $scope.srefUrl = function(e) {
      return '_pages/docs/api/' + e.sref + '.ngt';
    }
  }

  m.controller('DocumentationCtrl', DocumentationCtrl);

  m.config(["$stateProvider", function($stateProvider) {
    $stateProvider
      .state('docs.api', {
        abstract: true,
        url: '/api',
        template: '<ui-view></ui-view>'
      })
      .state('docs.api.index', {
        url: '',
        templateUrl: '_pages/docs/api.ngt',
        controller: DocumentationCtrl
      });

    for (var i = 0; i < endpoints.length; i++) {
      var e = endpoints[i];

      $stateProvider.state('docs.api.' + e.sref, {
        url: '/' + e.sref,
        templateUrl: '_pages/docs/api/' + e.sref + '.ngt'
      });
    }

    for (var i = 0; i < types.length; i++) {
      var t = types[i];

      $stateProvider.state('docs.api.type-' + t.id, {
        url: '/type-' + t.id,
        templateUrl: '_pages/docs/api/type-' + t.id + '.ngt'
      });
    }
  }]);
})();


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

  m.config(["$stateProvider", "$urlRouterProvider", "$locationProvider", "githubProvider", "$uiViewScrollProvider", function($stateProvider, $urlRouterProvider, $locationProvider, githubProvider, $uiViewScrollProvider) {
    $locationProvider.html5Mode(false).hashPrefix('!');
    $urlRouterProvider.otherwise("/index");
    githubProvider.setUrl('https://github.com/spotify/heroic');
  }]);

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

  m.directive('gitHrefPackage', ["github", function(github) {
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
  }]);

  m.directive('gitHrefJava', ["github", function(github) {
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
  }]);

  m.directive('gitHref', ["github", function(github) {
    return {
      restrict: 'A',
      link: function($scope, $element, $attr) {
        $element.attr('href', github.relativeUrl($attr.gitHref));
      }
    };
  }]);

  m.directive('id', ["$state", function($state) {
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
  }]);

  /**
   * Helper directive for creating indented code blocks.
   *
   * Will setup, and compile a code block containing <pre><code language=...>.
   *
   * @param content Specify dynamic content of the code block.
   * @param language Specify the language of the code block.
   */
  m.directive('codeblock', ["$compile", function($compile) {
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
  }]);
})();
