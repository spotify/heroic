/**
 * Define a grammar called Hello
 */
grammar HeroicQuery;

queries
    : (query QuerySeparator)* query EOF
    ;

expressionOnly
    : expr EOF
    ;

filterOnly
    : filter EOF
    ;

query
    : select from? where?
    ;

select
    : All  # SelectAll
    | expr # SelectAggregation
    ;

from
    : From Identifier sourceRange?
    ;

where
    : Where filter
    ;

filter
    : LParen filter RParen #FilterPrecedence
    | filter Or filter     #FilterOr
    | filter And filter    #FilterAnd
    | expr Eq expr         #FilterEq
    | expr NotEq expr      #FilterNotEq
    | SKey Eq expr         #FilterKeyEq
    | SKey NotEq expr      #FilterKeyNotEq
    | Plus expr            #FilterHas
    | expr Prefix expr     #FilterPrefix
    | expr NotPrefix expr  #FilterNotPrefix
    | expr Regex expr      #FilterRegex
    | expr NotRegex expr   #FilterNotRegex
    | expr In expr         #FilterIn
    | expr Not In expr     #FilterNotIn
    | (True | False)       #FilterBoolean
    | Bang filter          #FilterNot
    ;

string
    : QuotedString
    | SimpleString
    | Identifier
    ;

keyValue
    : Identifier Eq expr
    ;

expr
    : LParen expr RParen                                                  #ExpressionPrecedence
    | expr Minus expr                                                     #ExpressionMinus
    | expr Plus expr                                                      #ExpressionPlus
    | LBracket (expr (Comma expr)*)? RBracket                             #ExpressionList
    | LCurly (expr (Comma expr)*)? RCurly                                 #ExpressionList
    | SNow                                                                #ExpressionNow
    | Duration                                                            #ExpressionDuration
    | Integer                                                             #ExpressionInteger
    | Float                                                               #ExpressionFloat
    | string                                                              #ExpressionString
    | expr By expr                                                        #AggregationBy
    | expr By All                                                         #AggregationByAll
    | expr (Pipe expr)+                                                   #AggregationPipe
    | Identifier (LParen (expr (Comma expr)*)? (Comma keyValue)* RParen)? #Aggregation
    ;

sourceRange
    : LParen expr RParen            #SourceRangeRelative
    | LParen expr Comma expr RParen #SourceRangeAbsolute
    ;

// keywords (must come before SimpleString!)
All : '*' ;

True : 'true' ;

False : 'false' ;

Where : 'where' ;

From : 'from' ;

Or : 'or' ;

And : 'and' ;

Not : 'not' ;

In : 'in' ;

By : 'by' ;

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

Comma : ',' ;

LParen : '(' ;

RParen : ')' ;

LCurly : '{' ;

RCurly : '}' ;

LBracket : '[' ;

RBracket : ']' ;

Pipe : '|' ;

QuotedString : '"' StringCharacters? '"' ;

Identifier : [a-zA-Z] [a-zA-Z0-9]* ;

// strings that do not have to be quoted
SimpleString : [a-zA-Z] [a-zA-Z0-9:/_\-\.]* ;

SKey : '$key' ;

SNow : '$now' ;

Duration
    : Integer Unit
    ;

Integer
    : Digits
    ;

Float
    : Digits '.' Digits?
    | '.' Digits
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

fragment
Unit
    : 'ms'
    | 's'
    | 'm'
    | 'H' | 'h'
    | 'd'
    | 'w'
    | 'M'
    | 'y'
    ;

fragment
Digits
    : [0-9]+
    ;

WS : [ \t\n]+ -> skip ;

// is used to specifically match string where the end quote is missing
UnterminatedQutoedString : '"' StringCharacters? ;

// match everything else so that we can handle errors in the parser.
ErrorChar
    : .
    ;