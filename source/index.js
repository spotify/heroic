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
    $locationProvider.html5Mode(false).hashPrefix('!');
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

  m.directive('a', function($location, $anchorScroll) {
    return {
      restrict: 'E',
      link: function($scope, $element, $attr) {
        if (!$attr.href)
          return;

        var href = $attr.href;

        if (href[0] !== '#')
          return;

        if (href[1] === '!')
          return;

        var hash = href.substring(1, href.length);

        $element.on('click', function(e) {
          e.preventDefault();
          var old = $location.hash();
          $location.hash(hash);
          $anchorScroll();
          $location.hash(old);
          return false;
        });
      }
    };
  });
})();
