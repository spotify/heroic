(function() {
  var m = angular.module('hdoc.tutorial', [
    '_pages/tutorial/index.ngt',
    '_pages/tutorial/kafka_consumer.ngt',
  ]);

  m.config(function($stateProvider) {
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
  });
})();
