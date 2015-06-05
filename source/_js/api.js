(function() {
  var m = angular.module('hdoc.api', [
    '_js/api-endpoint.ngt',
    '_js/api-response.ngt',
    '_js/api-accept.ngt'
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
      controller: function ApiResponseCtrl($scope) {
        $scope.status = $scope.status || '200';
        $scope.contentType = $scope.contentType || 'application/json';
        $scope.showDoc = false;
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
