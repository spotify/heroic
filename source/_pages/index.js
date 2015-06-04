(function() {
  var m = angular.module('hdoc.index', [
    '_pages/index.ngt'
  ]);

  m.config(function($stateProvider) {
    $stateProvider
      .state('index', {
        url: "/index",
        templateUrl: "_pages/index.ngt"
      });
    });
})();
