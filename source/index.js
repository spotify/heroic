//= include '../bower_components/angular/angular.js'
//= include '../bower_components/angular-ui-bootstrap-bower/ui-bootstrap.js'
//= include '../bower_components/angular-ui-router/release/angular-ui-router.js'

//= include 'templates.js'
//= include '_js/prism.js'
//= include '_pages/*.js'

(function() {
  var m = angular.module('hdoc', [
    'ui.router',
    'hdoc.landing',
    'hdoc.docs'
  ]);

  function HeroicDocumentationCtrl() {
  }

  m.controller('HeroicDocumentationCtrl', HeroicDocumentationCtrl);

  m.config(function($stateProvider, $urlRouterProvider, $locationProvider) {
    $locationProvider.html5Mode(true);
    $urlRouterProvider.otherwise("/landing");
  });

  m.directive('code', function() {
    return {
      restrict: 'E',
      link: function($scope, $element, $attr) {
        if (!$attr.language)
          return;

        $element.addClass('language-' + $attr.language);
        Prism.highlightElement($element[0]);
      }
    };
  });
})();
