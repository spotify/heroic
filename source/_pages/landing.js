(function() {
  var m = angular.module('hdoc.landing', [
    '_pages/landing.ngt'
  ]);

  m.config(function($stateProvider) {
    $stateProvider
      .state('landing', {
        url: "/landing",
        templateUrl: "_pages/landing.ngt"
      });
    });
})();
