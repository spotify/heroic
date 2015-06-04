Prism.languages.hql = {
  'string': /("(?!:)(\\?[^'"])*?"(?!:)|[a-z\.]+)/g
};

Prism.languages.insertBefore('hql', 'string', {
  'operator': /(\+|and|or|!=|=|!)/g,
  'keyword': /(\$key|\$now|<[a-z]+>)/g
});
