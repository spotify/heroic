(function() {
  var m = angular.module('hdoc.docs', [
    '_pages/docs.ngt',
    '_pages/docs/architecture.ngt',
    '_pages/docs/getting_started.ngt',
    '_pages/docs/data_model.ngt'
  ]);

  function DocumentationCtrl() {
  }

  m.controller('DocumentationCtrl', DocumentationCtrl);

  m.config(function($stateProvider) {
    $stateProvider
      .state('docs', {
        url: "/docs",
        templateUrl: "_pages/docs.ngt"
      })
      .state('docs.architecture', {
        url: '/architecture',
        templateUrl: '_pages/docs/architecture.ngt'
      })
      .state('docs.getting_started', {
        url: '/getting_started',
        templateUrl: '_pages/docs/getting_started.ngt'
      })
      .state('docs.data_model', {
        url: '/data_model',
        templateUrl: '_pages/docs/data_model.ngt'
      });
  });
})();
