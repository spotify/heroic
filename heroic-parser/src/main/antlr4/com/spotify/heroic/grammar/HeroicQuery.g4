grammar HeroicQuery;

statements
    : (statement StatementSeparator)* statement? EOF
    ;

statement
    : Let expr Eq query #LetStatement
    | query             #QueryStatement
    ;

query
    : select=expr from? where? with? as?
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
    : LParen filter RParen           #FilterPrecedence
    | left=filter Or right=filter    #FilterOr
    | left=filter And right=filter   #FilterAnd
    | key=expr Eq value=expr         #FilterEq
    | key=expr NotEq value=expr      #FilterNotEq
    | SKey Eq expr                   #FilterKeyEq
    | SKey NotEq expr                #FilterKeyNotEq
    | Plus expr                      #FilterHas
    | key=expr Prefix value=expr     #FilterPrefix
    | key=expr NotPrefix value=expr  #FilterNotPrefix
    | key=expr Regex value=expr      #FilterRegex
    | key=expr NotRegex value=expr   #FilterNotRegex
    | key=expr In value=expr         #FilterIn
    | key=expr Not In value=expr     #FilterNotIn
    | (True | False)                 #FilterBoolean
    | Bang filter                    #FilterNot
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
    | Duration                                                            #ExpressionDuration
    | Integer                                                             #ExpressionInteger
    | Float                                                               #ExpressionFloat
    | string                                                              #ExpressionString
    | Reference                                                           #ExpressionReference
    | LCurly TimeLiteral RCurly                                           #ExpressionTime
    | LCurly DateTimeLiteral RCurly                                       #ExpressionDateTime
    | LBracket (expr (Comma expr)*)? RBracket                             #ExpressionList
    | LCurly (expr (Comma expr)*)? RCurly                                 #ExpressionList
    | Minus expr                                                          #ExpressionNegate
    | left=expr operator=(Div | Mul) right=expr                           #ExpressionDivMul
    | left=expr operator=(Plus | Minus) right=expr                        #ExpressionPlusMinus
    | expression=expr By grouping=expr                                    #ExpressionBy
    | expr (Pipe expr)+                                                   #ExpressionPipe
    | Identifier (LParen (expr (Comma expr)*)? (Comma keyValue)* RParen)? #ExpressionFunction
    | Identifier (LParen (keyValue (Comma keyValue)*)? RParen)?           #ExpressionFunction
    | Mul                                                                 #ExpressionAny
    ;

sourceRange
    : LParen distance=expr RParen             #SourceRangeRelative
    | LParen start=expr Comma end=expr RParen #SourceRangeAbsolute
    ;

// keywords (must come before other literals)
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

// strings that do not have to be quoted
SimpleString
    : [a-zA-Z0-9:/_\-\.]+
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
