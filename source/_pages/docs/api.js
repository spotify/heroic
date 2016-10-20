(function() {
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

  m.config(function($stateProvider) {
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
  });
})();
