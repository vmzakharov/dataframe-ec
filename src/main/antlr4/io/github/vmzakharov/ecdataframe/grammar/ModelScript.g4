grammar ModelScript;

script :
      functionDeclarationExpr*
      statementSequence
      projectionStatement?
    ;

functionDeclarationExpr :
    'function' ID ('(' idList ')' | '(' ')')?
    '{'
        statementSequence
    '}'
    ;

statementSequence: statement*;

statement :
      ID '=' expr     #assignExpr
    | IF expr
      THEN ifBody=statementSequence
      (ELSE elseBody=statementSequence)?
      ENDIF           #conditionExpr
    | expr            #freeExp
    ;

projectionStatement :
    'project'? '{' exprList '}' ('where' expr)?
    ;

expr :
      ID ( '.' ID)+?           #propertyPathExpr
    | ID  ':' expr             #aliasExpr
    | ID ('(' exprList ')' | '(' ')') #functionCallExpr
    | '(' expr ')'             #parenExpr
    | '-' expr                 #unaryMinusExpr
    | expr IS_NOT_EMPTY        #isNotEmptyExpr
    | expr IS_EMPTY            #isEmptyExpr
    | NOT expr                 #notExpr
    | expr '[' expr ']'        #indexVectorExpr
    | expr op=(MUL | DIV) expr #mulDivExpr
    | expr op=(ADD | SUB) expr #addSubExpr
    | expr op=(GT | GTE | LT | LTE | EQ | NE) expr #compareExpr
    | expr (NOT_IN | IN) expr  #inExpr
    | expr op=AND expr         #andExpr
    | expr op=(OR | XOR) expr  #orExpr
    | vectorExpr               #standaloneVectorExpr
    | INT                      #intLiteralExpr
    | DOUBLE                   #doubleLiteralExpr
    | STRING                   #stringLiteralExpr
    | ID                       #varExpr
    | condExpr=expr '?' ifExpr=expr ':' elseExpr=expr   #ternaryExpr
    ;

vectorExpr : '(' exprList ')' | '(' ')';

exprList :  expr ( ',' expr )*;

idList : ID ( ',' ID) *;

MUL : '*' ;
DIV : '/' ;
ADD : '+' ;
SUB : '-' ;

GT  : '>' ;
GTE : '>=' ;
LT  : '<' ;
LTE : '<=' ;
EQ  : '==';
NE  : '!=';

NOT_IN  : 'not in' | 'NOT IN' ;
IN  : 'in' | 'IN' ;
IS_EMPTY  : 'is empty' | 'IS EMPTY' ;
IS_NOT_EMPTY  : 'is not empty' | 'IS NOT EMPTY' ;

AND  : 'and' | 'AND';
OR   : 'or' | 'OR';
XOR  : 'xor' | 'XOR';

NOT : 'not' | 'NOT' | '!';

IF    : 'if' | 'IF';
THEN  : 'then' | 'THEN';
ELSE  : 'else' | 'ELSE';
ENDIF : 'endif' | 'ENDIF';

ID : LETTER (LETTER|DIGIT)*;
INT : DIGIT+;
DOUBLE : ('.' DIGIT+ | DIGIT+ ('.' DIGIT*)? ) ;
WS : [ \t]+ -> skip;
NL : [\n\r] -> skip;
STRING : '"' ( ESC | . )*? '"' | '\'' ( ESC | . )*? '\'';

fragment
ESC : '\\' [btnr"\\];
LETTER : [a-zA-Z\u0080-\u00FF_];
DIGIT  : [0-9];