/**
 * Define a grammar called Hello
 */
grammar HeroicQuery;

queries
    : (query QuerySeparator)* query EOF
    ;

query
    : select From from (Where filter)? (GroupBy groupBy)?
    ;

eqExpr
    : valueExpr Eq valueExpr
    ;

notEqExpr
    : valueExpr NotEq valueExpr
    ;

keyEqExpr
    : SKey Eq valueExpr
    ;

keyNotEqExpr
    : SKey NotEq valueExpr
    ;

hasExpr
    : Plus valueExpr
    ;

prefixExpr
    : valueExpr Prefix valueExpr
    ;

notPrefixExpr
    : valueExpr NotPrefix valueExpr
    ;

regexExpr
    : valueExpr Regex valueExpr
    ;

notInExpr
    : valueExpr Not In valueExpr
    ;

booleanExpr
    : True
    | False
    ;

inExpr
    : valueExpr In valueExpr
    ;

notRegexExpr
    : valueExpr NotRegex valueExpr
    ;

notExpr
    : Bang filterExprs
    ;

filterExpr
    : eqExpr
    | notEqExpr
    | keyEqExpr
    | keyNotEqExpr
    | hasExpr
    | prefixExpr
    | notPrefixExpr
    | regexExpr
    | notRegexExpr
    | inExpr
    | notInExpr
    | booleanExpr
    ;

groupExpr
    : LParen filterExprs RParen
    ;

filterExprs
    : filterExpr
    | notExpr
    | groupExpr
    | <assoc=left> filterExprs And filterExprs
    | <assoc=left> filterExprs Or filterExprs
    ;

filter
    : filterExprs
    ;

listValues
    : valueExpr (Colon valueExpr)*
    ;

groupBy
    : listValues
    ;

list
    : LBracket listValues? RBracket
    ;

keyValue
    : Identifier Eq valueExpr
    ;

string
    : QuotedString
    | SimpleString
    | Identifier
    ;

aggregationArgs
    : listValues (Colon keyValue)*
    | keyValue (Colon keyValue)*
    ;

aggregation
    : string LParen aggregationArgs? RParen
    ;

placeholder
    : Placeholder
    ;

value
    : now
    | diff
    | placeholder
    | aggregation
    | list
    | integer
    | string
    ;

diff
    : Diff
    ;

now
    : SNow
    ;

integer: Integer ;

groupValueExpr
    : LParen valueExpr RParen ;

valueExpr
    : value
    | groupValueExpr
    |<assoc=right> valueExpr Plus valueExpr
    |<assoc=right> valueExpr Minus valueExpr
    ;

select
    : All
    | valueExpr
    ;

relative
    : LParen valueExpr RParen
    ;

absolute
    : LParen valueExpr Colon valueExpr RParen ;

sourceRange
    : relative
    | absolute
    ;

from : Identifier sourceRange? ;

// keywords (must come before SimpleString!)
All : '*' ;

True : 'true' ;

False : 'false' ;

Where : 'where' ;

GroupBy : 'group by' ;

From : 'from' ;

Or : 'or' ;

And : 'and' ;

Not : 'not' ;

In : 'in' ;

Plus : '+' ;

Minus : '-' ;

Eq : '=' ;

Regex : '~' ;

NotRegex : '!~' ;

Prefix : '^' ;

NotPrefix : '!^' ;

Bang : '!' ;

NotEq : '!=' ;

QuerySeparator : ';' ;

Colon : ',' ;

LParen : '(' ;

RParen : ')' ;

LCurly : '}' ;

RCurly : '}' ;

LBracket : '[' ;

RBracket : ']' ;

Placeholder : LCurly Identifier RCurly ;

QuotedString : '"' StringCharacters? '"' ;

Identifier : [a-zA-Z] [a-zA-Z0-9]* ;

// strings that do not have to be quoted
SimpleString : [a-zA-Z] [a-zA-Z0-9:/_\-\.]* ;

SKey : '$key' ;

SNow : '$now' ;

fragment
Unit
    : 'ms'
    | 's'
    | 'm'
    | 'H'
    | 'd'
    | 'w'
    | 'M'
    | 'y'
    ;

Diff
    : Integer Unit
    ;

Integer
    : '0'
    | [1-9] [0-9]*
    ;

fragment
StringCharacters
    : StringCharacter+
    ;

fragment
StringCharacter
    : ~["\\]
    | EscapeSequence
    ;

fragment
EscapeSequence
    : '\\' [btnfr"'\\]
    ;

WS : [ \t\n]+ -> skip ;

// is used to specifically match string where the end quote is missing
UnterminatedQutoedString : '"' StringCharacters? ;

// match everything else so that we can handle errors in the parser.
ErrorChar : . ;