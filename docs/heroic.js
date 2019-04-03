var _self = (typeof window !== 'undefined')
	? window   // if in browser
	: (
		(typeof WorkerGlobalScope !== 'undefined' && self instanceof WorkerGlobalScope)
		? self // if in worker
		: {}   // if in node js
	);

/**
 * Prism: Lightweight, robust, elegant syntax highlighting
 * MIT license http://www.opensource.org/licenses/mit-license.php/
 * @author Lea Verou http://lea.verou.me
 */

var Prism = (function(){

// Private helper vars
var lang = /\blang(?:uage)?-(\w+)\b/i;
var uniqueId = 0;

var _ = _self.Prism = {
	util: {
		encode: function (tokens) {
			if (tokens instanceof Token) {
				return new Token(tokens.type, _.util.encode(tokens.content), tokens.alias);
			} else if (_.util.type(tokens) === 'Array') {
				return tokens.map(_.util.encode);
			} else {
				return tokens.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/\u00a0/g, ' ');
			}
		},

		type: function (o) {
			return Object.prototype.toString.call(o).match(/\[object (\w+)\]/)[1];
		},

		objId: function (obj) {
			if (!obj['__id']) {
				Object.defineProperty(obj, '__id', { value: ++uniqueId });
			}
			return obj['__id'];
		},

		// Deep clone a language definition (e.g. to extend it)
		clone: function (o) {
			var type = _.util.type(o);

			switch (type) {
				case 'Object':
					var clone = {};

					for (var key in o) {
						if (o.hasOwnProperty(key)) {
							clone[key] = _.util.clone(o[key]);
						}
					}

					return clone;

				case 'Array':
					// Check for existence for IE8
					return o.map && o.map(function(v) { return _.util.clone(v); });
			}

			return o;
		}
	},

	languages: {
		extend: function (id, redef) {
			var lang = _.util.clone(_.languages[id]);

			for (var key in redef) {
				lang[key] = redef[key];
			}

			return lang;
		},

		/**
		 * Insert a token before another token in a language literal
		 * As this needs to recreate the object (we cannot actually insert before keys in object literals),
		 * we cannot just provide an object, we need anobject and a key.
		 * @param inside The key (or language id) of the parent
		 * @param before The key to insert before. If not provided, the function appends instead.
		 * @param insert Object with the key/value pairs to insert
		 * @param root The object that contains `inside`. If equal to Prism.languages, it can be omitted.
		 */
		insertBefore: function (inside, before, insert, root) {
			root = root || _.languages;
			var grammar = root[inside];

			if (arguments.length == 2) {
				insert = arguments[1];

				for (var newToken in insert) {
					if (insert.hasOwnProperty(newToken)) {
						grammar[newToken] = insert[newToken];
					}
				}

				return grammar;
			}

			var ret = {};

			for (var token in grammar) {

				if (grammar.hasOwnProperty(token)) {

					if (token == before) {

						for (var newToken in insert) {

							if (insert.hasOwnProperty(newToken)) {
								ret[newToken] = insert[newToken];
							}
						}
					}

					ret[token] = grammar[token];
				}
			}

			// Update references in other language definitions
			_.languages.DFS(_.languages, function(key, value) {
				if (value === root[inside] && key != inside) {
					this[key] = ret;
				}
			});

			return root[inside] = ret;
		},

		// Traverse a language definition with Depth First Search
		DFS: function(o, callback, type, visited) {
			visited = visited || {};
			for (var i in o) {
				if (o.hasOwnProperty(i)) {
					callback.call(o, i, o[i], type || i);

					if (_.util.type(o[i]) === 'Object' && !visited[_.util.objId(o[i])]) {
						visited[_.util.objId(o[i])] = true;
						_.languages.DFS(o[i], callback, null, visited);
					}
					else if (_.util.type(o[i]) === 'Array' && !visited[_.util.objId(o[i])]) {
						visited[_.util.objId(o[i])] = true;
						_.languages.DFS(o[i], callback, i, visited);
					}
				}
			}
		}
	},
	plugins: {},

	highlightAll: function(async, callback) {
		var env = {
			callback: callback,
			selector: 'code[class*="language-"], [class*="language-"] code, code[class*="lang-"], [class*="lang-"] code'
		};

		_.hooks.run("before-highlightall", env);

		var elements = env.elements || document.querySelectorAll(env.selector);

		for (var i=0, element; element = elements[i++];) {
			_.highlightElement(element, async === true, env.callback);
		}
	},

	highlightElement: function(element, async, callback) {
		// Find language
		var language, grammar, parent = element;

		while (parent && !lang.test(parent.className)) {
			parent = parent.parentNode;
		}

		if (parent) {
			language = (parent.className.match(lang) || [,''])[1].toLowerCase();
			grammar = _.languages[language];
		}

		// Set language on the element, if not present
		element.className = element.className.replace(lang, '').replace(/\s+/g, ' ') + ' language-' + language;

		// Set language on the parent, for styling
		parent = element.parentNode;

		if (/pre/i.test(parent.nodeName)) {
			parent.className = parent.className.replace(lang, '').replace(/\s+/g, ' ') + ' language-' + language;
		}

		var code = element.textContent;

		var env = {
			element: element,
			language: language,
			grammar: grammar,
			code: code
		};

		_.hooks.run('before-sanity-check', env);

		if (!env.code || !env.grammar) {
			_.hooks.run('complete', env);
			return;
		}

		_.hooks.run('before-highlight', env);

		if (async && _self.Worker) {
			var worker = new Worker(_.filename);

			worker.onmessage = function(evt) {
				env.highlightedCode = evt.data;

				_.hooks.run('before-insert', env);

				env.element.innerHTML = env.highlightedCode;

				callback && callback.call(env.element);
				_.hooks.run('after-highlight', env);
				_.hooks.run('complete', env);
			};

			worker.postMessage(JSON.stringify({
				language: env.language,
				code: env.code,
				immediateClose: true
			}));
		}
		else {
			env.highlightedCode = _.highlight(env.code, env.grammar, env.language);

			_.hooks.run('before-insert', env);

			env.element.innerHTML = env.highlightedCode;

			callback && callback.call(element);

			_.hooks.run('after-highlight', env);
			_.hooks.run('complete', env);
		}
	},

	highlight: function (text, grammar, language) {
		var tokens = _.tokenize(text, grammar);
		return Token.stringify(_.util.encode(tokens), language);
	},

	tokenize: function(text, grammar, language) {
		var Token = _.Token;

		var strarr = [text];

		var rest = grammar.rest;

		if (rest) {
			for (var token in rest) {
				grammar[token] = rest[token];
			}

			delete grammar.rest;
		}

		tokenloop: for (var token in grammar) {
			if(!grammar.hasOwnProperty(token) || !grammar[token]) {
				continue;
			}

			var patterns = grammar[token];
			patterns = (_.util.type(patterns) === "Array") ? patterns : [patterns];

			for (var j = 0; j < patterns.length; ++j) {
				var pattern = patterns[j],
					inside = pattern.inside,
					lookbehind = !!pattern.lookbehind,
					greedy = !!pattern.greedy,
					lookbehindLength = 0,
					alias = pattern.alias;

				pattern = pattern.pattern || pattern;

				for (var i=0; i<strarr.length; i++) { // Don’t cache length as it changes during the loop

					var str = strarr[i];

					if (strarr.length > text.length) {
						// Something went terribly wrong, ABORT, ABORT!
						break tokenloop;
					}

					if (str instanceof Token) {
						continue;
					}

					pattern.lastIndex = 0;

					var match = pattern.exec(str),
					    delNum = 1;

					// Greedy patterns can override/remove up to two previously matched tokens
					if (!match && greedy && i != strarr.length - 1) {
						// Reconstruct the original text using the next two tokens
						var nextToken = strarr[i + 1].matchedStr || strarr[i + 1],
						    combStr = str + nextToken;

						if (i < strarr.length - 2) {
							combStr += strarr[i + 2].matchedStr || strarr[i + 2];
						}

						// Try the pattern again on the reconstructed text
						pattern.lastIndex = 0;
						match = pattern.exec(combStr);
						if (!match) {
							continue;
						}

						var from = match.index + (lookbehind ? match[1].length : 0);
						// To be a valid candidate, the new match has to start inside of str
						if (from >= str.length) {
							continue;
						}
						var to = match.index + match[0].length,
						    len = str.length + nextToken.length;

						// Number of tokens to delete and replace with the new match
						delNum = 3;

						if (to <= len) {
							if (strarr[i + 1].greedy) {
								continue;
							}
							delNum = 2;
							combStr = combStr.slice(0, len);
						}
						str = combStr;
					}

					if (!match) {
						continue;
					}

					if(lookbehind) {
						lookbehindLength = match[1].length;
					}

					var from = match.index + lookbehindLength,
					    match = match[0].slice(lookbehindLength),
					    to = from + match.length,
					    before = str.slice(0, from),
					    after = str.slice(to);

					var args = [i, delNum];

					if (before) {
						args.push(before);
					}

					var wrapped = new Token(token, inside? _.tokenize(match, inside) : match, alias, match, greedy);

					args.push(wrapped);

					if (after) {
						args.push(after);
					}

					Array.prototype.splice.apply(strarr, args);
				}
			}
		}

		return strarr;
	},

	hooks: {
		all: {},

		add: function (name, callback) {
			var hooks = _.hooks.all;

			hooks[name] = hooks[name] || [];

			hooks[name].push(callback);
		},

		run: function (name, env) {
			var callbacks = _.hooks.all[name];

			if (!callbacks || !callbacks.length) {
				return;
			}

			for (var i=0, callback; callback = callbacks[i++];) {
				callback(env);
			}
		}
	}
};

var Token = _.Token = function(type, content, alias, matchedStr, greedy) {
	this.type = type;
	this.content = content;
	this.alias = alias;
	// Copy of the full string this token was created from
	this.matchedStr = matchedStr || null;
	this.greedy = !!greedy;
};

Token.stringify = function(o, language, parent) {
	if (typeof o == 'string') {
		return o;
	}

	if (_.util.type(o) === 'Array') {
		return o.map(function(element) {
			return Token.stringify(element, language, o);
		}).join('');
	}

	var env = {
		type: o.type,
		content: Token.stringify(o.content, language, parent),
		tag: 'span',
		classes: ['token', o.type],
		attributes: {},
		language: language,
		parent: parent
	};

	if (env.type == 'comment') {
		env.attributes['spellcheck'] = 'true';
	}

	if (o.alias) {
		var aliases = _.util.type(o.alias) === 'Array' ? o.alias : [o.alias];
		Array.prototype.push.apply(env.classes, aliases);
	}

	_.hooks.run('wrap', env);

	var attributes = '';

	for (var name in env.attributes) {
		attributes += (attributes ? ' ' : '') + name + '="' + (env.attributes[name] || '') + '"';
	}

	return '<' + env.tag + ' class="' + env.classes.join(' ') + '" ' + attributes + '>' + env.content + '</' + env.tag + '>';

};

if (!_self.document) {
	if (!_self.addEventListener) {
		// in Node.js
		return _self.Prism;
	}
 	// In worker
	_self.addEventListener('message', function(evt) {
		var message = JSON.parse(evt.data),
		    lang = message.language,
		    code = message.code,
		    immediateClose = message.immediateClose;

		_self.postMessage(_.highlight(code, _.languages[lang], lang));
		if (immediateClose) {
			_self.close();
		}
	}, false);

	return _self.Prism;
}

//Get current script and highlight
var script = document.currentScript || [].slice.call(document.getElementsByTagName("script")).pop();

if (script) {
	_.filename = script.src;

	if (document.addEventListener && !script.hasAttribute('data-manual')) {
		if(document.readyState !== "loading") {
			requestAnimationFrame(_.highlightAll, 0);
		}
		else {
			document.addEventListener('DOMContentLoaded', _.highlightAll);
		}
	}
}

return _self.Prism;

})();

if (typeof module !== 'undefined' && module.exports) {
	module.exports = Prism;
}

// hack for components to work correctly in node.js
if (typeof global !== 'undefined') {
	global.Prism = Prism;
}


/* **********************************************
     Begin prism-markup.js
********************************************** */

Prism.languages.markup = {
	'comment': /<!--[\w\W]*?-->/,
	'prolog': /<\?[\w\W]+?\?>/,
	'doctype': /<!DOCTYPE[\w\W]+?>/,
	'cdata': /<!\[CDATA\[[\w\W]*?]]>/i,
	'tag': {
		pattern: /<\/?(?!\d)[^\s>\/=.$<]+(?:\s+[^\s>\/=]+(?:=(?:("|')(?:\\\1|\\?(?!\1)[\w\W])*\1|[^\s'">=]+))?)*\s*\/?>/i,
		inside: {
			'tag': {
				pattern: /^<\/?[^\s>\/]+/i,
				inside: {
					'punctuation': /^<\/?/,
					'namespace': /^[^\s>\/:]+:/
				}
			},
			'attr-value': {
				pattern: /=(?:('|")[\w\W]*?(\1)|[^\s>]+)/i,
				inside: {
					'punctuation': /[=>"']/
				}
			},
			'punctuation': /\/?>/,
			'attr-name': {
				pattern: /[^\s>\/]+/,
				inside: {
					'namespace': /^[^\s>\/:]+:/
				}
			}

		}
	},
	'entity': /&#?[\da-z]{1,8};/i
};

// Plugin to make entity title show the real entity, idea by Roman Komarov
Prism.hooks.add('wrap', function(env) {

	if (env.type === 'entity') {
		env.attributes['title'] = env.content.replace(/&amp;/, '&');
	}
});

Prism.languages.xml = Prism.languages.markup;
Prism.languages.html = Prism.languages.markup;
Prism.languages.mathml = Prism.languages.markup;
Prism.languages.svg = Prism.languages.markup;


/* **********************************************
     Begin prism-css.js
********************************************** */

Prism.languages.css = {
	'comment': /\/\*[\w\W]*?\*\//,
	'atrule': {
		pattern: /@[\w-]+?.*?(;|(?=\s*\{))/i,
		inside: {
			'rule': /@[\w-]+/
			// See rest below
		}
	},
	'url': /url\((?:(["'])(\\(?:\r\n|[\w\W])|(?!\1)[^\\\r\n])*\1|.*?)\)/i,
	'selector': /[^\{\}\s][^\{\};]*?(?=\s*\{)/,
	'string': /("|')(\\(?:\r\n|[\w\W])|(?!\1)[^\\\r\n])*\1/,
	'property': /(\b|\B)[\w-]+(?=\s*:)/i,
	'important': /\B!important\b/i,
	'function': /[-a-z0-9]+(?=\()/i,
	'punctuation': /[(){};:]/
};

Prism.languages.css['atrule'].inside.rest = Prism.util.clone(Prism.languages.css);

if (Prism.languages.markup) {
	Prism.languages.insertBefore('markup', 'tag', {
		'style': {
			pattern: /(<style[\w\W]*?>)[\w\W]*?(?=<\/style>)/i,
			lookbehind: true,
			inside: Prism.languages.css,
			alias: 'language-css'
		}
	});

	Prism.languages.insertBefore('inside', 'attr-value', {
		'style-attr': {
			pattern: /\s*style=("|').*?\1/i,
			inside: {
				'attr-name': {
					pattern: /^\s*style/i,
					inside: Prism.languages.markup.tag.inside
				},
				'punctuation': /^\s*=\s*['"]|['"]\s*$/,
				'attr-value': {
					pattern: /.+/i,
					inside: Prism.languages.css
				}
			},
			alias: 'language-css'
		}
	}, Prism.languages.markup.tag);
}

/* **********************************************
     Begin prism-clike.js
********************************************** */

Prism.languages.clike = {
	'comment': [
		{
			pattern: /(^|[^\\])\/\*[\w\W]*?\*\//,
			lookbehind: true
		},
		{
			pattern: /(^|[^\\:])\/\/.*/,
			lookbehind: true
		}
	],
	'string': {
		pattern: /(["'])(\\(?:\r\n|[\s\S])|(?!\1)[^\\\r\n])*\1/,
		greedy: true
	},
	'class-name': {
		pattern: /((?:\b(?:class|interface|extends|implements|trait|instanceof|new)\s+)|(?:catch\s+\())[a-z0-9_\.\\]+/i,
		lookbehind: true,
		inside: {
			punctuation: /(\.|\\)/
		}
	},
	'keyword': /\b(if|else|while|do|for|return|in|instanceof|function|new|try|throw|catch|finally|null|break|continue)\b/,
	'boolean': /\b(true|false)\b/,
	'function': /[a-z0-9_]+(?=\()/i,
	'number': /\b-?(?:0x[\da-f]+|\d*\.?\d+(?:e[+-]?\d+)?)\b/i,
	'operator': /--?|\+\+?|!=?=?|<=?|>=?|==?=?|&&?|\|\|?|\?|\*|\/|~|\^|%/,
	'punctuation': /[{}[\];(),.:]/
};


/* **********************************************
     Begin prism-javascript.js
********************************************** */

Prism.languages.javascript = Prism.languages.extend('clike', {
	'keyword': /\b(as|async|await|break|case|catch|class|const|continue|debugger|default|delete|do|else|enum|export|extends|finally|for|from|function|get|if|implements|import|in|instanceof|interface|let|new|null|of|package|private|protected|public|return|set|static|super|switch|this|throw|try|typeof|var|void|while|with|yield)\b/,
	'number': /\b-?(0x[\dA-Fa-f]+|0b[01]+|0o[0-7]+|\d*\.?\d+([Ee][+-]?\d+)?|NaN|Infinity)\b/,
	// Allow for all non-ASCII characters (See http://stackoverflow.com/a/2008444)
	'function': /[_$a-zA-Z\xA0-\uFFFF][_$a-zA-Z0-9\xA0-\uFFFF]*(?=\()/i
});

Prism.languages.insertBefore('javascript', 'keyword', {
	'regex': {
		pattern: /(^|[^/])\/(?!\/)(\[.+?]|\\.|[^/\\\r\n])+\/[gimyu]{0,5}(?=\s*($|[\r\n,.;})]))/,
		lookbehind: true,
		greedy: true
	}
});

Prism.languages.insertBefore('javascript', 'string', {
	'template-string': {
		pattern: /`(?:\\\\|\\?[^\\])*?`/,
		greedy: true,
		inside: {
			'interpolation': {
				pattern: /\$\{[^}]+\}/,
				inside: {
					'interpolation-punctuation': {
						pattern: /^\$\{|\}$/,
						alias: 'punctuation'
					},
					rest: Prism.languages.javascript
				}
			},
			'string': /[\s\S]+/
		}
	}
});

if (Prism.languages.markup) {
	Prism.languages.insertBefore('markup', 'tag', {
		'script': {
			pattern: /(<script[\w\W]*?>)[\w\W]*?(?=<\/script>)/i,
			lookbehind: true,
			inside: Prism.languages.javascript,
			alias: 'language-javascript'
		}
	});
}

Prism.languages.js = Prism.languages.javascript;

/* **********************************************
     Begin prism-file-highlight.js
********************************************** */

(function () {
	if (typeof self === 'undefined' || !self.Prism || !self.document || !document.querySelector) {
		return;
	}

	self.Prism.fileHighlight = function() {

		var Extensions = {
			'js': 'javascript',
			'py': 'python',
			'rb': 'ruby',
			'ps1': 'powershell',
			'psm1': 'powershell',
			'sh': 'bash',
			'bat': 'batch',
			'h': 'c',
			'tex': 'latex'
		};

		if(Array.prototype.forEach) { // Check to prevent error in IE8
			Array.prototype.slice.call(document.querySelectorAll('pre[data-src]')).forEach(function (pre) {
				var src = pre.getAttribute('data-src');

				var language, parent = pre;
				var lang = /\blang(?:uage)?-(?!\*)(\w+)\b/i;
				while (parent && !lang.test(parent.className)) {
					parent = parent.parentNode;
				}

				if (parent) {
					language = (pre.className.match(lang) || [, ''])[1];
				}

				if (!language) {
					var extension = (src.match(/\.(\w+)$/) || [, ''])[1];
					language = Extensions[extension] || extension;
				}

				var code = document.createElement('code');
				code.className = 'language-' + language;

				pre.textContent = '';

				code.textContent = 'Loading…';

				pre.appendChild(code);

				var xhr = new XMLHttpRequest();

				xhr.open('GET', src, true);

				xhr.onreadystatechange = function () {
					if (xhr.readyState == 4) {

						if (xhr.status < 400 && xhr.responseText) {
							code.textContent = xhr.responseText;

							Prism.highlightElement(code);
						}
						else if (xhr.status >= 400) {
							code.textContent = '✖ Error ' + xhr.status + ' while fetching file: ' + xhr.statusText;
						}
						else {
							code.textContent = '✖ Error: File does not exist or is empty';
						}
					}
				};

				xhr.send(null);
			});
		}

	};

	document.addEventListener('DOMContentLoaded', self.Prism.fileHighlight);

})();

(function(Prism) {
	var insideString = {
		variable: [
			// Arithmetic Environment
			{
				pattern: /\$?\(\([\w\W]+?\)\)/,
				inside: {
					// If there is a $ sign at the beginning highlight $(( and )) as variable
					variable: [{
							pattern: /(^\$\(\([\w\W]+)\)\)/,
							lookbehind: true
						},
						/^\$\(\(/,
					],
					number: /\b-?(?:0x[\dA-Fa-f]+|\d*\.?\d+(?:[Ee]-?\d+)?)\b/,
					// Operators according to https://www.gnu.org/software/bash/manual/bashref.html#Shell-Arithmetic
					operator: /--?|-=|\+\+?|\+=|!=?|~|\*\*?|\*=|\/=?|%=?|<<=?|>>=?|<=?|>=?|==?|&&?|&=|\^=?|\|\|?|\|=|\?|:/,
					// If there is no $ sign at the beginning highlight (( and )) as punctuation
					punctuation: /\(\(?|\)\)?|,|;/
				}
			},
			// Command Substitution
			{
				pattern: /\$\([^)]+\)|`[^`]+`/,
				inside: {
					variable: /^\$\(|^`|\)$|`$/
				}
			},
			/\$(?:[a-z0-9_#\?\*!@]+|\{[^}]+\})/i
		],
	};

	Prism.languages.bash = {
		'shebang': {
			pattern: /^#!\s*\/bin\/bash|^#!\s*\/bin\/sh/,
			alias: 'important'
		},
		'comment': {
			pattern: /(^|[^"{\\])#.*/,
			lookbehind: true
		},
		'string': [
			//Support for Here-Documents https://en.wikipedia.org/wiki/Here_document
			{
				pattern: /((?:^|[^<])<<\s*)(?:"|')?(\w+?)(?:"|')?\s*\r?\n(?:[\s\S])*?\r?\n\2/g,
				lookbehind: true,
				greedy: true,
				inside: insideString
			},
			{
				pattern: /(["'])(?:\\\\|\\?[^\\])*?\1/g,
				greedy: true,
				inside: insideString
			}
		],
		'variable': insideString.variable,
		// Originally based on http://ss64.com/bash/
		'function': {
			pattern: /(^|\s|;|\||&)(?:alias|apropos|apt-get|aptitude|aspell|awk|basename|bash|bc|bg|builtin|bzip2|cal|cat|cd|cfdisk|chgrp|chmod|chown|chroot|chkconfig|cksum|clear|cmp|comm|command|cp|cron|crontab|csplit|cut|date|dc|dd|ddrescue|df|diff|diff3|dig|dir|dircolors|dirname|dirs|dmesg|du|egrep|eject|enable|env|ethtool|eval|exec|expand|expect|export|expr|fdformat|fdisk|fg|fgrep|file|find|fmt|fold|format|free|fsck|ftp|fuser|gawk|getopts|git|grep|groupadd|groupdel|groupmod|groups|gzip|hash|head|help|hg|history|hostname|htop|iconv|id|ifconfig|ifdown|ifup|import|install|jobs|join|kill|killall|less|link|ln|locate|logname|logout|look|lpc|lpr|lprint|lprintd|lprintq|lprm|ls|lsof|make|man|mkdir|mkfifo|mkisofs|mknod|more|most|mount|mtools|mtr|mv|mmv|nano|netstat|nice|nl|nohup|notify-send|nslookup|open|op|passwd|paste|pathchk|ping|pkill|popd|pr|printcap|printenv|printf|ps|pushd|pv|pwd|quota|quotacheck|quotactl|ram|rar|rcp|read|readarray|readonly|reboot|rename|renice|remsync|rev|rm|rmdir|rsync|screen|scp|sdiff|sed|seq|service|sftp|shift|shopt|shutdown|sleep|slocate|sort|source|split|ssh|stat|strace|su|sudo|sum|suspend|sync|tail|tar|tee|test|time|timeout|times|touch|top|traceroute|trap|tr|tsort|tty|type|ulimit|umask|umount|unalias|uname|unexpand|uniq|units|unrar|unshar|uptime|useradd|userdel|usermod|users|uuencode|uudecode|v|vdir|vi|vmstat|wait|watch|wc|wget|whereis|which|who|whoami|write|xargs|xdg-open|yes|zip)(?=$|\s|;|\||&)/,
			lookbehind: true
		},
		'keyword': {
			pattern: /(^|\s|;|\||&)(?:let|:|\.|if|then|else|elif|fi|for|break|continue|while|in|case|function|select|do|done|until|echo|exit|return|set|declare)(?=$|\s|;|\||&)/,
			lookbehind: true
		},
		'boolean': {
			pattern: /(^|\s|;|\||&)(?:true|false)(?=$|\s|;|\||&)/,
			lookbehind: true
		},
		'operator': /&&?|\|\|?|==?|!=?|<<<?|>>|<=?|>=?|=~/,
		'punctuation': /\$?\(\(?|\)\)?|\.\.|[{}[\];]/
	};

	var inside = insideString.variable[1].inside;
	inside['function'] = Prism.languages.bash['function'];
	inside.keyword = Prism.languages.bash.keyword;
	inside.boolean = Prism.languages.bash.boolean;
	inside.operator = Prism.languages.bash.operator;
	inside.punctuation = Prism.languages.bash.punctuation;
})(Prism);
Prism.languages.css = {
	'comment': /\/\*[\w\W]*?\*\//,
	'atrule': {
		pattern: /@[\w-]+?.*?(;|(?=\s*\{))/i,
		inside: {
			'rule': /@[\w-]+/
			// See rest below
		}
	},
	'url': /url\((?:(["'])(\\(?:\r\n|[\w\W])|(?!\1)[^\\\r\n])*\1|.*?)\)/i,
	'selector': /[^\{\}\s][^\{\};]*?(?=\s*\{)/,
	'string': /("|')(\\(?:\r\n|[\w\W])|(?!\1)[^\\\r\n])*\1/,
	'property': /(\b|\B)[\w-]+(?=\s*:)/i,
	'important': /\B!important\b/i,
	'function': /[-a-z0-9]+(?=\()/i,
	'punctuation': /[(){};:]/
};

Prism.languages.css['atrule'].inside.rest = Prism.util.clone(Prism.languages.css);

if (Prism.languages.markup) {
	Prism.languages.insertBefore('markup', 'tag', {
		'style': {
			pattern: /(<style[\w\W]*?>)[\w\W]*?(?=<\/style>)/i,
			lookbehind: true,
			inside: Prism.languages.css,
			alias: 'language-css'
		}
	});

	Prism.languages.insertBefore('inside', 'attr-value', {
		'style-attr': {
			pattern: /\s*style=("|').*?\1/i,
			inside: {
				'attr-name': {
					pattern: /^\s*style/i,
					inside: Prism.languages.markup.tag.inside
				},
				'punctuation': /^\s*=\s*['"]|['"]\s*$/,
				'attr-value': {
					pattern: /.+/i,
					inside: Prism.languages.css
				}
			},
			alias: 'language-css'
		}
	}, Prism.languages.markup.tag);
}
Prism.languages.sql= {
	'comment': {
		pattern: /(^|[^\\])(?:\/\*[\w\W]*?\*\/|(?:--|\/\/|#).*)/,
		lookbehind: true
	},
	'string' : {
		pattern: /(^|[^@\\])("|')(?:\\?[\s\S])*?\2/,
		lookbehind: true
	},
	'variable': /@[\w.$]+|@("|'|`)(?:\\?[\s\S])+?\1/,
	'function': /\b(?:COUNT|SUM|AVG|MIN|MAX|FIRST|LAST|UCASE|LCASE|MID|LEN|ROUND|NOW|FORMAT)(?=\s*\()/i, // Should we highlight user defined functions too?
	'keyword': /\b(?:ACTION|ADD|AFTER|ALGORITHM|ALL|ALTER|ANALYZE|ANY|APPLY|AS|ASC|AUTHORIZATION|AUTO_INCREMENT|BACKUP|BDB|BEGIN|BERKELEYDB|BIGINT|BINARY|BIT|BLOB|BOOL|BOOLEAN|BREAK|BROWSE|BTREE|BULK|BY|CALL|CASCADED?|CASE|CHAIN|CHAR VARYING|CHARACTER (?:SET|VARYING)|CHARSET|CHECK|CHECKPOINT|CLOSE|CLUSTERED|COALESCE|COLLATE|COLUMN|COLUMNS|COMMENT|COMMIT|COMMITTED|COMPUTE|CONNECT|CONSISTENT|CONSTRAINT|CONTAINS|CONTAINSTABLE|CONTINUE|CONVERT|CREATE|CROSS|CURRENT(?:_DATE|_TIME|_TIMESTAMP|_USER)?|CURSOR|DATA(?:BASES?)?|DATE(?:TIME)?|DBCC|DEALLOCATE|DEC|DECIMAL|DECLARE|DEFAULT|DEFINER|DELAYED|DELETE|DELIMITER(?:S)?|DENY|DESC|DESCRIBE|DETERMINISTIC|DISABLE|DISCARD|DISK|DISTINCT|DISTINCTROW|DISTRIBUTED|DO|DOUBLE(?: PRECISION)?|DROP|DUMMY|DUMP(?:FILE)?|DUPLICATE KEY|ELSE|ENABLE|ENCLOSED BY|END|ENGINE|ENUM|ERRLVL|ERRORS|ESCAPE(?:D BY)?|EXCEPT|EXEC(?:UTE)?|EXISTS|EXIT|EXPLAIN|EXTENDED|FETCH|FIELDS|FILE|FILLFACTOR|FIRST|FIXED|FLOAT|FOLLOWING|FOR(?: EACH ROW)?|FORCE|FOREIGN|FREETEXT(?:TABLE)?|FROM|FULL|FUNCTION|GEOMETRY(?:COLLECTION)?|GLOBAL|GOTO|GRANT|GROUP|HANDLER|HASH|HAVING|HOLDLOCK|IDENTITY(?:_INSERT|COL)?|IF|IGNORE|IMPORT|INDEX|INFILE|INNER|INNODB|INOUT|INSERT|INT|INTEGER|INTERSECT|INTO|INVOKER|ISOLATION LEVEL|JOIN|KEYS?|KILL|LANGUAGE SQL|LAST|LEFT|LIMIT|LINENO|LINES|LINESTRING|LOAD|LOCAL|LOCK|LONG(?:BLOB|TEXT)|MATCH(?:ED)?|MEDIUM(?:BLOB|INT|TEXT)|MERGE|MIDDLEINT|MODIFIES SQL DATA|MODIFY|MULTI(?:LINESTRING|POINT|POLYGON)|NATIONAL(?: CHAR VARYING| CHARACTER(?: VARYING)?| VARCHAR)?|NATURAL|NCHAR(?: VARCHAR)?|NEXT|NO(?: SQL|CHECK|CYCLE)?|NONCLUSTERED|NULLIF|NUMERIC|OFF?|OFFSETS?|ON|OPEN(?:DATASOURCE|QUERY|ROWSET)?|OPTIMIZE|OPTION(?:ALLY)?|ORDER|OUT(?:ER|FILE)?|OVER|PARTIAL|PARTITION|PERCENT|PIVOT|PLAN|POINT|POLYGON|PRECEDING|PRECISION|PREV|PRIMARY|PRINT|PRIVILEGES|PROC(?:EDURE)?|PUBLIC|PURGE|QUICK|RAISERROR|READ(?:S SQL DATA|TEXT)?|REAL|RECONFIGURE|REFERENCES|RELEASE|RENAME|REPEATABLE|REPLICATION|REQUIRE|RESTORE|RESTRICT|RETURNS?|REVOKE|RIGHT|ROLLBACK|ROUTINE|ROW(?:COUNT|GUIDCOL|S)?|RTREE|RULE|SAVE(?:POINT)?|SCHEMA|SELECT|SERIAL(?:IZABLE)?|SESSION(?:_USER)?|SET(?:USER)?|SHARE MODE|SHOW|SHUTDOWN|SIMPLE|SMALLINT|SNAPSHOT|SOME|SONAME|START(?:ING BY)?|STATISTICS|STATUS|STRIPED|SYSTEM_USER|TABLES?|TABLESPACE|TEMP(?:ORARY|TABLE)?|TERMINATED BY|TEXT(?:SIZE)?|THEN|TIMESTAMP|TINY(?:BLOB|INT|TEXT)|TOP?|TRAN(?:SACTIONS?)?|TRIGGER|TRUNCATE|TSEQUAL|TYPES?|UNBOUNDED|UNCOMMITTED|UNDEFINED|UNION|UNIQUE|UNPIVOT|UPDATE(?:TEXT)?|USAGE|USE|USER|USING|VALUES?|VAR(?:BINARY|CHAR|CHARACTER|YING)|VIEW|WAITFOR|WARNINGS|WHEN|WHERE|WHILE|WITH(?: ROLLUP|IN)?|WORK|WRITE(?:TEXT)?)\b/i,
	'boolean': /\b(?:TRUE|FALSE|NULL)\b/i,
	'number': /\b-?(?:0x)?\d*\.?[\da-f]+\b/,
	'operator': /[-+*\/=%^~]|&&?|\|?\||!=?|<(?:=>?|<|>)?|>[>=]?|\b(?:AND|BETWEEN|IN|LIKE|NOT|OR|IS|DIV|REGEXP|RLIKE|SOUNDS LIKE|XOR)\b/i,
	'punctuation': /[;[\]()`,.]/
};
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

Prism.languages.json = {
    'operator': /<[a-z]+>/g,
    'keyword': /"(\b|\B)[\w-]+"(?=\s*:)/ig,
    'string': /"(?!:)(\\?[^'"])*?"(?!:)/g,
    'number': /\b-?(0x[\dA-Fa-f]+|\d*\.?\d+([Ee]-?\d+)?)\b/g,
    'punctuation': /[{}[\]);,]/g,
    'boolean': /\b(true|false)\b/gi,
    'null': /\bnull\b/gi
};

Prism.languages.hql = {
  'string': /("(?!:)(\\?[^'"])*?"(?!:)|\b[a-z\.]+\b)/g
};

Prism.languages.insertBefore('hql', 'string', {
  'operator': /(\+|-|!=|=|!\^|\^|!)|(\b(and|or|not|in|with|as|by)\b)/g,
  'keyword': /(\$[a-z]+|\$now|<[a-z]+>|\b[0-9]+(ms|s|m|H|d|w)\b)/g
});

Prism.languages.ts = {
  'keyword': /[a-z\-]+(?=[=])/g,
  'number': /[a-z\.]+(?=\s*{)/g,
  'string': /(?:(?![=]))[a-z\-%\/]+/g,
  'operator': /(=)/g
};


(function() {
  var m = angular.module('hdoc.api', [
    '_js/api-endpoint.ngt',
    '_js/api-response.ngt',
    '_js/api-accept.ngt',
    '_js/api-type.ngt'
  ]);

  function nameToType(name) {
    if (!name)
      throw new Error('name must be defined');

    name = name.replace(/([a-z])([A-Z])/g, '$1-$2').toLowerCase();
    name = name.replace(/[\._]/g, '-');
    return 'type-' + name;
  }

  function pathToId(method, path) {
    var parts = path.split('/');

    var id = [method.toLowerCase()];

    for (var i = 1, l = parts.length; i < l; i++) {
      id.push(parts[i]);
    }

    return id.join('-');
  }

  m.directive('apiEndpoint', function() {
    return {
      scope: {},
      restrict: 'E',
      transclude: true,
      replace: true,
      templateUrl: '_js/api-endpoint.ngt',
      require: 'apiEndpoint',
      link: function($scope, $element, $attr, ctrl) {
        ctrl.path = $attr.path || '/';
        ctrl.method = $attr.method || 'GET';
        $scope.id = pathToId(ctrl.method, ctrl.path);
        $scope.path = ctrl.path;
        $scope.method = ctrl.method;
      },
      controller: ["$scope", function($scope) {
      }]
    };
  });

  m.directive('apiResponse', function() {
    return {
      scope: {status: '@', contentType: '@'},
      restrict: 'E',
      transclude: true,
      replace: true,
      templateUrl: '_js/api-response.ngt',
      require: '^apiEndpoint',
      link: function($scope, $element, $attr, endpoint) {
        $scope.status = $scope.status || '200';
        $scope.contentType = $scope.contentType || 'application/json';
      }
    };
  });

  m.directive('apiTypeId', function() {
    return {
      link: function($scope, $element, $attr) {
        $element.attr('id', nameToType($attr.apiTypeId));
      }
    };
  });

  m.directive('apiType', function() {
    return {
      scope: {},
      restrict: 'E',
      transclude: true,
      templateUrl: '_js/api-type.ngt',
      replace: true,
      compile: function($element, $attr, transclude) {
        return function($scope, $element, $attr) {
          $scope.structural = $attr.kind === 'structural';
          $scope.name = $attr.name || null;
          $scope.id = $scope.name !== null ? nameToType($scope.name) : null;
        };
      },
      controller: ["$scope", function ApiTypeCtrl($scope) {
        $scope.fields = [];

        this.addField = function(field) {
          $scope.fields.push(field);
        };
      }]
    };
  });

  m.directive('apiFieldBind', ["$compile", function($compile) {
    return {
      link: function($scope, $element, $attr) {
        $scope.$watch($attr.apiFieldBind, function(element) {
          if (!element)
            return;

          $element.children().remove();
          $element.append(element);
          $compile(element)($scope);
        });
      }
    };
  }]);

  m.directive('apiField', ["$sce", "$state", function($sce, $state) {
    function compileTypeHref(typeHref) {
      var code = angular.element('<code>');
      var a = angular.element('<a>');
      var glyph = angular.element('<span class="glyphicon glyphicon-link">');
      var smallGlyph = angular.element('<small>');
      smallGlyph.append(glyph);

      var href = $state.href('^.' + nameToType(typeHref));

      a.attr('href', href);
      code.text(typeHref);

      a.append(code);
      a.append(smallGlyph);

      return a;
    }

    function compileTypeArrayHref(typeArrayHref) {
      var code = angular.element('<code>');
      var a = angular.element('<a>');
      var glyph = angular.element('<span class="glyphicon glyphicon-link">');
      var smallGlyph = angular.element('<small>');
      smallGlyph.append(glyph);

      var href = $state.href('^.' + nameToType(typeArrayHref));

      a.attr('href', href);
      code.text("[" + typeArrayHref + ", ...]");

      a.append(code);
      a.append(smallGlyph);

      return a;
    }

    function compileTypeJson(typeJson) {
      var code = angular.element('<code language="json">');

      code.text(typeJson);

      return code;
    }

    function compileType($attr) {
      if (!!$attr.typeHref)
        return compileTypeHref($attr.typeHref);

      if (!!$attr.typeArrayHref)
        return compileTypeArrayHref($attr.typeArrayHref);

      if (!!$attr.typeJson)
        return compileTypeJson($attr.typeJson);

      var noType = angular.element('<em>');
      noType.text('no type');
      return noType;
    }

    return {
      require: '^apiType',
      compile: function($element, $attr, $transclude) {
        return function($scope, $element, $attr, type) {
          var compiledType = compileType($attr);

          var field = {
            name: $attr.name,
            required: $attr.required === 'true' ? true : false,
            type: compiledType,
            purpose: $element.clone().contents()
          };

          $element.remove();
          type.addField(field);
        };
      }
    };
  }]);

  m.directive('apiAccept', ["$sce", function($sce) {
    return {
      restrict: 'E',
      transclude: true,
      replace: true,
      templateUrl: '_js/api-accept.ngt',
      require: '^apiEndpoint',
      link: function($scope, $element, $attr, endpoint) {
        $scope.contentType = $attr.contentType || 'application/json';
        $scope.curl = null;
        $scope.isEmpty = true;
        $scope.curlData = $attr.curlData || '{}';

        var buildCurl = function(contentType, isEmpty) {
          var curl = "$ curl";

          if ((endpoint.method !== 'GET' || !isEmpty) && endpoint.method !== 'POST')
            curl += " -X" + endpoint.method;

          if (!isEmpty)
            curl += " -H \"Content-Type: " + $scope.contentType + "\"";

          curl += " http://localhost:8080" + endpoint.path;

          if (!isEmpty)
            curl += " \\\n  -d '" + $scope.curlData + "'";

          return $sce.trustAsHtml(curl);
        };

        $scope.$watch($attr.empty, function(empty) {
          $scope.isEmpty = !!empty;
          $scope.curl = buildCurl($scope.contentType, $scope.isEmpty);
        });
      }
    };
  }]);
})();

(function() {
  DocumentationCtrl.$inject = ["$scope"];
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

  m.config(["$stateProvider", function($stateProvider) {
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
  }]);
})();

(function() {
  var m = angular.module('hdoc.index', [
    '_pages/index.ngt'
  ]);

  m.config(["$stateProvider", function($stateProvider) {
    $stateProvider
      .state('index', {
        url: "/index",
        templateUrl: "_pages/index.ngt"
      });
    }]);
})();

(function() {
  var m = angular.module('hdoc.tutorial', [
    '_pages/tutorial/index.ngt',
    '_pages/tutorial/kafka_consumer.ngt',
  ]);

  m.config(["$stateProvider", function($stateProvider) {
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
  }]);
})();

(function() {
  DocumentationCtrl.$inject = ["$scope"];
  var endpointUrls = endpoints.map(function(e) {
    return '_pages/docs/api/' + e.sref + '.ngt';
  });

  var typeUrls = types.map(function(t) {
    return '_pages/docs/api/type-' + t.id + '.ngt';
  });

  var m = angular.module('hdoc.docs.api', [
    '_pages/docs/api.ngt',
    '_pages/docs/api/accept-metadata-query-body.ngt',
    '_pages/docs/api/accept-series.ngt',
  ].concat(endpointUrls).concat(typeUrls));

  function DocumentationCtrl($scope) {
    $scope.endpoints = endpoints;
    $scope.types = types;

    $scope.srefUrl = function(e) {
      return '_pages/docs/api/' + e.sref + '.ngt';
    }
  }

  m.controller('DocumentationCtrl', DocumentationCtrl);

  m.config(["$stateProvider", function($stateProvider) {
    $stateProvider
      .state('docs.api', {
        abstract: true,
        url: '/api',
        template: '<ui-view></ui-view>'
      })
      .state('docs.api.index', {
        url: '',
        templateUrl: '_pages/docs/api.ngt',
        controller: DocumentationCtrl
      });

    for (var i = 0; i < endpoints.length; i++) {
      var e = endpoints[i];

      $stateProvider.state('docs.api.' + e.sref, {
        url: '/' + e.sref,
        templateUrl: '_pages/docs/api/' + e.sref + '.ngt'
      });
    }

    for (var i = 0; i < types.length; i++) {
      var t = types[i];

      $stateProvider.state('docs.api.type-' + t.id, {
        url: '/type-' + t.id,
        templateUrl: '_pages/docs/api/type-' + t.id + '.ngt'
      });
    }
  }]);
})();


(function() {
  var m = angular.module('hdoc', [
    'ui.router',
    'ui.bootstrap',
    'hdoc.index',
    'hdoc.docs',
    'hdoc.tutorial',
    'hdoc.api'
  ]);

  var LINESEP = /[\n]/;

  function stripText($element, removeIndent) {
    removeIndent = removeIndent || false;

    var text = $element.text();
    var prefix = null;

    if (removeIndent) {
      var previous = $element[0].previousSibling;

      // if previous node is text.
      if (previous !== null && previous.nodeType === 3) {
        var parts = previous.nodeValue.split(LINESEP);
        prefix = parts[parts.length - 1];
      }
    }

    var lines = text.split(LINESEP);

    var result;

    if (prefix === null) {
      result = lines;
    } else {
      result = [];

      for (var i = 0; i < lines.length; i++) {
        var line = lines[i];

        if (line.length < prefix.length) {
          result.push(line);
          continue;
        }

        result.push(line.substring(prefix.length, line.length));
      }
    }

    var index = 0;

    while (result[index] === "") {
      index++;
    }

    return result.slice(index).join('\n');
  }

  function HeroicDocumentationCtrl() {
  }

  m.controller('HeroicDocumentationCtrl', HeroicDocumentationCtrl);

  m.config(["$stateProvider", "$urlRouterProvider", "$locationProvider", "githubProvider", "$uiViewScrollProvider", function($stateProvider, $urlRouterProvider, $locationProvider, githubProvider, $uiViewScrollProvider) {
    $locationProvider.html5Mode(false).hashPrefix('!');
    $urlRouterProvider.otherwise("/index");
    githubProvider.setUrl('https://github.com/spotify/heroic');
  }]);

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

        $element.text(stripText($element));
        $element.addClass('language-' + $attr.language);
        Prism.highlightElement($element[0]);
      }
    };
  });

  m.directive('gitHrefPackage', ["github", function(github) {
    return {
      restrict: 'A',
      link: function($scope, $element, $attr) {
        var href = $attr.gitHrefPackage;
        var colon = href.indexOf(':');

        var component, path;

        if (colon == -1) {
          component = href;
          path = $element.text();
        } else {
          component = href.substring(0, colon);
          path = href.substring(colon + 1, href.length);
        }

        path = path.replace(/\./g, '/');
        var newHref = component + '/src/main/java/' + path;
        $element.attr('href', github.blobUrl(newHref));
      }
    };
  }]);

  m.directive('gitHrefJava', ["github", function(github) {
    return {
      restrict: 'A',
      link: function($scope, $element, $attr) {
        var href = $attr.gitHrefJava;
        var colon = href.indexOf(':');

        var component, path;

        if (colon == -1) {
          component = href;
          path = $element.text();
        } else {
          component = href.substring(0, colon);
          path = href.substring(colon + 1, href.length);
        }

        path = path.replace(/\./g, '/') + '.java';
        var newHref = component + '/src/main/java/' + path;
        $element.attr('href', github.blobUrl(newHref));
      }
    };
  }]);

  m.directive('gitHref', ["github", function(github) {
    return {
      restrict: 'A',
      link: function($scope, $element, $attr) {
        $element.attr('href', github.relativeUrl($attr.gitHref));
      }
    };
  }]);

  m.directive('id', ["$state", function($state) {
    return {
      restrict: 'A',
      link: function($scope, $element, $attr) {
        var $glyph = angular.element('<span class="glyphicon glyphicon-link">');

        var $ln = angular.element('<a class="link-to">');
        $ln.append($glyph);

        $element.append($ln);

        $scope.$watch(function() {
          return $attr.id;
        }, function(newId) {
          $ln.attr('href', $state.href('.', {'#': newId}));
        });
      }
    };
  }]);

  /**
   * Helper directive for creating indented code blocks.
   *
   * Will setup, and compile a code block containing <pre><code language=...>.
   *
   * @param content Specify dynamic content of the code block.
   * @param language Specify the language of the code block.
   */
  m.directive('codeblock', ["$compile", function($compile) {
    return {
      link: function($scope, $element, $attr) {
        var children = $element.children();
        var pre = angular.element('<pre>');
        var code = angular.element('<code>');

        if (!!$attr.language)
          code.attr('language', $attr.language);

        pre.append(code);

        if (!!$attr.content) {
          $element.replaceWith(pre);

          $scope.$watch($attr.content, function(content) {
            content = content || '';
            code.text(content);
            $compile(pre)($scope);
          });

          return;
        }

        var text = stripText($element, true);

        $element.replaceWith(pre);
        code.text(text);
        $compile(pre)($scope);
      }
    };
  }]);
})();
