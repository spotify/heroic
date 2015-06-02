Prism.languages.yaml = {
  'string': /"(?!:)(\\?[^'"])*?"(?!:)/g,
  'prolog': /(---|\.\.\.)[^\r\n]*(\r?\n|$)/g,
  'comment': /#[^\r\n]*(\r?\n|$)/g,
  'number': /\b-?(0x[\dA-Fa-f]+|\d*\.?\d+)\b/g,
  'keyword': /[a-zA-Z0-9_-]+\:/gi
};

Prism.languages.insertBefore('yaml', 'keyword', {
  'important': {
    pattern: /\s+(\||\>|-)/g,
    inside: {
      'important': /(\||\>|-)/
    },
    rest: Prism.languages.yaml
  },
  'keyword': /(&#38;|&amp;|&\z|\*)[\w]+/
});
