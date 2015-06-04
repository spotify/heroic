//= include '../bower_components/angular/angular.js'
//= include '../bower_components/angular-ui-bootstrap-bower/ui-bootstrap.js'
//= include '../bower_components/angular-ui-router/release/angular-ui-router.js'

//= include 'templates.js'
//= include '_js/prism.js'
//= include '_pages/*.js'

(function() {
  var m = angular.module('hdoc', [
    'ui.router',
    'ui.bootstrap',
    'hdoc.index',
    'hdoc.docs'
  ]);

  function HeroicDocumentationCtrl() {
  }

  m.controller('HeroicDocumentationCtrl', HeroicDocumentationCtrl);

  m.config(function($stateProvider, $urlRouterProvider, $locationProvider, githubProvider) {
    $locationProvider.html5Mode(false).hashPrefix('!');
    $urlRouterProvider.otherwise("/index");
    githubProvider.setUrl('https://github.com/spotify/heroic');
  });

  m.provider('github', function() {
    var githubUrl = null;
    var githubBranch = 'master';

    this.setUrl = function(url) {
      githubUrl = url;
    };

    this.setBranch = function(branch) {
      githubBranch = branch;
    };

    this.$get = function() {
      return {
        url: githubUrl,
        relativeUrl: function(path) {
          return githubUrl + '/' + path;
        },
        blobUrl: function(path) {
          return githubUrl + '/blob/' + githubBranch + '/' + path;
        }
      };
    };
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

  m.directive('gitHrefJava', function(github) {
    return {
      restrict: 'A',
      link: function($scope, $element, $attr) {
        var href = $attr.gitHrefJava;
        var colon = href.indexOf(':');

        if (colon == -1)
          return;

        var component = href.substring(0, colon);
        var path = href.substring(colon + 1, href.length).replace(/\./g, '/') + '.java';
        var newHref = component + '/src/main/java/' + path;
        $element.attr('href', github.blobUrl(newHref));
      }
    };
  });

  m.directive('gitHref', function(github) {
    return {
      restrict: 'A',
      link: function($scope, $element, $attr) {
        $element.attr('href', github.relativeUrl($attr.gitHref));
      }
    };
  });
})();
