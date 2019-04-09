/* global Prism */

(function () {
  Prism.languages.ts = {
    'keyword': /[a-z\-]+(?=[=])/g,
    'number': /[a-z\.]+(?=\s*{)/g,
    'string': /(?:(?![=]))[a-z\-%\/]+/g,
    'operator': /(=)/g
  };
}());
