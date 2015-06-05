(function() {
  var m = angular.module('hdoc.api', [
    '_js/api-endpoint.ngt',
    '_js/api-response.ngt',
    '_js/api-accept.ngt',
    '_js/api-type.ngt'
  ]);

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
        $scope.path = ctrl.path;
        $scope.method = ctrl.method;
      },
      controller: function($scope) {
      }
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
        $scope.showDoc = false;
      }
    };
  });

  function nameToAnchor(name) {
    if (!name)
      throw new Error('name must be defined');

    name = name.replace(/([a-z])([A-Z])/g, '$1-$2').toLowerCase();
    name = name.replace(/[\._]/g, '-');
    return 'api-types-' + name;
  }

  m.directive('apiTypeId', function() {
    return {
      link: function($scope, $element, $attr) {
        $element.attr('id', nameToAnchor($attr.apiTypeId));
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
          $scope.id = $scope.name !== null ? nameToAnchor($scope.name) : null;
          $scope.showStructureDoc = $scope.name === null;
        };
      },
      controller: function ApiTypeCtrl($scope) {
        $scope.fields = [];

        this.addField = function(field) {
          $scope.fields.push(field);
        };
      }
    };
  });

  m.directive('apiFieldBind', function($compile) {
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
  });

  m.directive('apiField', function($sce, $state) {
    function compileTypeHref(typeHref) {
      var code = angular.element('<code>');
      var a = angular.element('<a>');
      var glyph = angular.element('<span class="glyphicon glyphicon-link">');
      var smallGlyph = angular.element('<small>');
      smallGlyph.append(glyph);

      var href = $state.href('.', {'#': nameToAnchor(typeHref)});

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

      var href = $state.href('.', {'#': nameToAnchor(typeArrayHref)});

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
  });

  m.directive('apiAccept', function($sce) {
    return {
      restrict: 'E',
      transclude: true,
      replace: true,
      templateUrl: '_js/api-accept.ngt',
      require: '^apiEndpoint',
      link: function($scope, $element, $attr, endpoint) {
        $scope.contentType = $attr.contentType || 'application/json';
        $scope.curl = null;
        $scope.showDoc = false;
        $scope.showCurl = false;
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
            curl += " -d '" + $scope.curlData + "'";

          return $sce.trustAsHtml(curl);
        };

        $scope.$watch($attr.empty, function(empty) {
          $scope.isEmpty = !!empty;
          $scope.curl = buildCurl($scope.contentType, $scope.isEmpty);
        });
      }
    };
  });
})();
