Prism.languages.json = {
    'operator': /<[a-z]+>/g,
    'keyword': /"(\b|\B)[\w-]+"(?=\s*:)/ig,
    'string': /"(?!:)(\\?[^'"])*?"(?!:)/g,
    'number': /\b-?(0x[\dA-Fa-f]+|\d*\.?\d+([Ee]-?\d+)?)\b/g,
    'punctuation': /[{}[\]);,]/g,
    'boolean': /\b(true|false)\b/gi,
    'null': /\bnull\b/gi
};
