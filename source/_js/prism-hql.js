Prism.languages.hql = {
  'operator': /(?=^|\W)(\+|=|!=|and|or)(?=\W)/ig,
  'keyword': /(\$key|\$now|\!|<[a-z]+>)/g,
  'string': /("(?!:)(\\?[^'"])*?"(?!:)|[a-z\.]+)/g
};
