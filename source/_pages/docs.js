(function() {
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

  m.config(function($stateProvider) {
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
  });
})();
