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
    '_pages/docs/filter_dsl.ngt',
    '_pages/docs/api.ngt',
    '_pages/docs/api/accept-metadata-query-body.ngt',
    '_pages/docs/api/accept-series.ngt',
    '_pages/docs/aggregations.ngt',
    '_pages/docs/shell.ngt',
    '_pages/docs/config.ngt',
    '_pages/docs/config/cluster.ngt',
    '_pages/docs/config/metadata.ngt',
    '_pages/docs/config/metrics.ngt',
    '_pages/docs/config/suggest.ngt',
    '_pages/docs/config/shell_server.ngt'
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
      })
      .state('docs.api', {
        url: '/api',
        templateUrl: '_pages/docs/api.ngt'
      })
      .state('docs.aggregations', {
        url: '/aggregations',
        templateUrl: '_pages/docs/aggregations.ngt'
      })
      .state('docs.shell', {
        url: '/shell',
        templateUrl: '_pages/docs/shell.ngt'
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
      .state('docs.config.shell_server', {
        url: '/shell_server',
        templateUrl: '_pages/docs/config/shell_server.ngt'
      });
  });
})();
