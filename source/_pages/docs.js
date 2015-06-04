(function() {
  var m = angular.module('hdoc.docs', [
    '_pages/docs.ngt',
    '_pages/docs/index.ngt',
    '_pages/docs/architecture.ngt',
    '_pages/docs/getting_started.ngt',
    '_pages/docs/getting_started/installation.ngt',
    '_pages/docs/getting_started/configuration.ngt',
    '_pages/docs/getting_started/compile.ngt',
    '_pages/docs/data_model.ngt',
    '_pages/docs/filter_dsl.ngt'
  ]);

  function DocumentationCtrl() {
  }

  m.controller('DocumentationCtrl', DocumentationCtrl);

  m.config(function($stateProvider) {
    $stateProvider
      .state('docs', {
        url: "/docs",
        templateUrl: "_pages/docs.ngt",
        controller: function($state) {
          if ($state.is('docs'))
            $state.go('docs.index');
        }
      })
      .state('docs.index', {
        url: "/index",
        templateUrl: "_pages/docs/index.ngt"
      })
      .state('docs.architecture', {
        url: '/architecture',
        templateUrl: '_pages/docs/architecture.ngt'
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
      .state('docs.filter_dsl', {
        url: '/filter_dsl',
        templateUrl: '_pages/docs/filter_dsl.ngt'
      })
      .state('docs.data_model', {
        url: '/data_model',
        templateUrl: '_pages/docs/data_model.ngt'
      });
  });
})();
