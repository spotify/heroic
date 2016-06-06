grammar HeroicQuery;

statements
    : (statement StatementSeparator)* statement? EOF
    ;

statement
    : Let expr Eq query #LetStatement
    | query             #QueryStatemnet
    ;

expressionOnly
    : expr EOF
    ;

filterOnly
    : filter EOF
    ;

query
    : select from? where? with? as?
    ;

select
    : Mul  # SelectAll
    | expr # SelectExpression
    ;

from
    : From Identifier sourceRange?
    ;

where
    : Where filter
    ;

with
    : With keyValues
    ;

as
    : As keyValues
    ;

keyValues
    : keyValue (Comma keyValue)*
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
    : LParen expr RParen                            #ExpressionPrecedence
    | Duration                                      #ExpressionDuration
    | Integer                                       #ExpressionInteger
    | Float                                         #ExpressionFloat
    | string                                        #ExpressionString
    | Reference                                     #ExpressionReference
    | LCurly TimeLiteral RCurly                     #ExpressionTime
    | LCurly DateTimeLiteral RCurly                 #ExpressionDateTime
    | LBracket (expr (Comma expr)*)? RBracket       #ExpressionList
    | LCurly (expr (Comma expr)*)? RCurly           #ExpressionList
    | Minus expr                                    #ExpressionNegate
    | expr (Div | Mul) expr                         #ExpressionDivMul
    | expr (Plus | Minus) expr                      #ExpressionPlusMinus
    | expr By expr                                  #AggregationBy
    | expr By Mul                                   #AggregationByAll
    | expr (Pipe expr)+                             #AggregationPipe
    | Identifier (LParen functionArguments RParen)? #ExpressionFunction
    ;

functionArguments
    : expr (Comma expr)* (Comma keyValue)*
    | keyValue (Comma keyValue)*
    ;

sourceRange
    : LParen expr RParen            #SourceRangeRelative
    | LParen expr Comma expr RParen #SourceRangeAbsolute
    ;

// keywords (must come before SimpleString!)
Let : 'let' ;

As : 'as' ;

True : 'true' ;

False : 'false' ;

Where : 'where' ;

With : 'with' ;

From : 'from' ;

Or : 'or' ;

And : 'and' ;

Not : 'not' ;

In : 'in' ;

By : 'by' ;

Plus : '+' ;

Minus : '-' ;

Div : '/' ;

Mul : '*' ;

Eq : '=' ;

Regex : '~' ;

NotRegex : '!~' ;

Prefix : '^' ;

NotPrefix : '!^' ;

Bang : '!' ;

NotEq : '!=' ;

StatementSeparator : ';' ;

Comma : ',' ;

LParen : '(' ;

RParen : ')' ;

LCurly : '{' ;

RCurly : '}' ;

LBracket : '[' ;

RBracket : ']' ;

Pipe : '|' ;

SKey : '$key' ;

// Only HH:MM:ss.SSS
TimeLiteral
    : [0-9]+ ':' [0-9]+ (':' [0-9]+ ('.' [0-9]+)? )?
    ;

// yyyy-MM-dd + TimeLiteral
DateTimeLiteral
    : [0-9]+ '-' [0-9]+ '-'  [0-9]+ (' ' TimeLiteral )?
    ;

Reference : '$' [a-zA-Z] [a-zA-Z0-9]* ;

QuotedString : '"' StringCharacters? '"' ;

Identifier : [a-zA-Z] [a-zA-Z0-9]* ;

// strings that do not have to be quoted
SimpleString : [a-zA-Z] [a-zA-Z0-9:/_\-\.]* ;

Duration
    : Minus? Integer Unit
    ;

Integer
    : Minus? Digits
    ;

Float
    : Minus? Digits '.' Digits?
    | Minus? '.' Digits
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
    | '\\u' [0-9a-f][0-9a-f][0-9a-f][0-9a-f]
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

LineComment
    : '#' ~[\r\n]* -> skip
    ;

Whitespace
    : [ \t\n\r]+ -> skip
    ;

// is used to specifically match string where the end quote is missing
UnterminatedQutoedString : '"' StringCharacters? ;

// match everything else so that we can handle errors in the parser.
ErrorChar
    : .
    ;
