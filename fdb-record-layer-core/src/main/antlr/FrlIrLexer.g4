/*
 * FrlIrLexer.g4
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

lexer grammar FrlIrLexer;

//
// FRL-IR is a Swift-like textual IR for Record Layer plans that translates to and from MLIR.
//
// The grammar contains no target-language code: the same grammar also generates the Swift parser. Turning parse trees
// into Record Layer objects is the job of visitors written for each target.
//
// Keywords are hard keywords and case-sensitive: `int` is a type, while `Int` is an identifier and `int` can never be a
// name.
//

// Primitive types, named as in the Record Layer's SQL dialect. `half`, `float`, and `double` are the 16-bit, 32-bit,
// and 64-bit floating-point types. `void` is the unit type, which Swift calls `Void`; it is the result type of a function
// that returns nothing.
BOOLEAN : 'boolean';
INT     : 'int';
BIGINT  : 'bigint';
HALF    : 'half';
FLOAT   : 'float';
DOUBLE  : 'double';
STRING  : 'string';
BYTES   : 'bytes';
UUID    : 'uuid';
VERSION : 'version';
VOID    : 'void';

// Generic types.
RECORD   : 'record';
ARRAY    : 'array';
STREAM   : 'stream';
VECTOR   : 'vector';
ENUM     : 'enum';
FUNCTION : 'function';

// Definitions and statements. `extern` declares a function or a type that the runtime provides, `typealias` names
// a type, `package` names the package that a program belongs to, and `yield` adds a value to the stream of a stream
// block, `stream { … }`, which `yield break` ends or stops. `return` leaves a function or a closure. `if`, `for`, and
// `consume` are the statements of control flow; `consume` pulls the elements of a stream, one `case` for each way a
// stream can answer. `overloaded` declares an overloaded function, and `variant` defines or declares one of its
// variants.
FUNC       : 'func';
EXTERN     : 'extern';
TYPE       : 'type';
TYPEALIAS  : 'typealias';
PACKAGE    : 'package';
IN         : 'in';
LET        : 'let';
YIELD      : 'yield';
BREAK      : 'break';
RETURN     : 'return';
IF         : 'if';
ELSE       : 'else';
FOR        : 'for';
CONSUME    : 'consume';
CASE       : 'case';
OVERLOADED : 'overloaded';
VARIANT    : 'variant';

// Literals that are keywords. `nil` is the null of every nullable type, as in Swift.
TRUE  : 'true';
FALSE : 'false';
NIL   : 'nil';

LT       : '<';
GT       : '>';
LPAREN   : '(';
RPAREN   : ')';
LBRACKET : '[';
RBRACKET : ']';
LBRACE   : '{';
RBRACE   : '}';
COMMA    : ',';
COLON    : ':';
SEMI     : ';';
EQUAL    : '=';
QUESTION : '?';
ARROW    : '->';
MINUS    : '-';
AT       : '@';
DOT      : '.';

// The operators besides `<`, `>`, and `-`. An operator is a call of the function that an `@operator` annotation gives
// it, see `expression` in FrlIrParser. `>=` is one token, so a generic type right before `=` needs a space:
// `let a: array<int> = b`.
PLUS        : '+';
STAR        : '*';
SLASH       : '/';
PERCENT     : '%';
BANG        : '!';
LE          : '<=';
GE          : '>=';
EQUAL_EQUAL : '==';
BANG_EQUAL  : '!=';

// A binary operator at the start of a line continues the expression of the line before if a space or a tab follows it,
// as in Swift. In `x` followed by a line `    - y`, the two lines hold one subtraction, `x - y`. In `x` followed by a
// line `-y`, the second line is a statement of its own, the negation `-y`. The lexer folds the line breaks before such
// an operator into the operator, with the blank lines and the comments between them, so that the parser gets no line
// break there; the token has the type of the operator. The rule is here because only the lexer sees whitespace. A line
// break after the operator does not count, because it can end a statement, as after the `>` of `record<…>` at the start
// of a line. An operator with type arguments, `+<int, int>`, is followed by its `<`, so it continues an expression only
// at the end of a line.
CONTINUED_PLUS        : LINE_START '+' OPERATOR_END -> type(PLUS);
CONTINUED_MINUS       : LINE_START '-' OPERATOR_END -> type(MINUS);
CONTINUED_STAR        : LINE_START '*' OPERATOR_END -> type(STAR);
CONTINUED_SLASH       : LINE_START '/' OPERATOR_END -> type(SLASH);
CONTINUED_PERCENT     : LINE_START '%' OPERATOR_END -> type(PERCENT);
CONTINUED_LT          : LINE_START '<' OPERATOR_END -> type(LT);
CONTINUED_LE          : LINE_START '<=' OPERATOR_END -> type(LE);
CONTINUED_GT          : LINE_START '>' OPERATOR_END -> type(GT);
CONTINUED_GE          : LINE_START '>=' OPERATOR_END -> type(GE);
CONTINUED_EQUAL_EQUAL : LINE_START '==' OPERATOR_END -> type(EQUAL_EQUAL);
CONTINUED_BANG_EQUAL  : LINE_START '!=' OPERATOR_END -> type(BANG_EQUAL);

// One or more line breaks, each followed by the whitespace and the comments of its line.
fragment LINE_START : (LINE_BREAK ([ \t] | LINE_COMMENT_TEXT | BLOCK_COMMENT_TEXT)*)+;

fragment OPERATOR_END : [ \t];

INTEGER_LITERAL : [0-9]+;

// As in Swift, a floating-point literal has digits on both sides of its point, an exponent, or both: `1.5`, `2e10`,
// `1.5e-3`.
FLOATING_LITERAL
    : [0-9]+ '.' [0-9]+ EXPONENT?
    | [0-9]+ EXPONENT
    ;

fragment EXPONENT : [eE] [+-]? [0-9]+;

// A string between double quotes, on one line, with the escape sequences of Swift: `\0`, `\\`, `\t`, `\n`, `\r`, `\"`,
// `\'`, and `\u{1F600}` for a Unicode scalar of one to eight hexadecimal digits.
STRING_LITERAL : '"' (~["\\\r\n] | ESCAPE_SEQUENCE)* '"';

fragment ESCAPE_SEQUENCE
    : '\\' [0\\tnr"']
    | '\\u{' [0-9a-fA-F]+ '}'
    ;

IDENTIFIER : [a-zA-Z_] [a-zA-Z_0-9]*;

// As in Swift, any text between backticks is a name, which lets a name be spelled like a keyword or contain other
// characters: `version`, `a.b`, `first name`. The backticks are not part of the name.
ESCAPED_IDENTIFIER : '`' ~[`\r\n]+ '`';

// Comments as in Swift: a line comment runs to the end of the line, and block comments nest. A comment is whitespace;
// the line break that ends a line comment is not part of it, so it still separates statements, while a line break
// inside a block comment does not.
LINE_COMMENT  : LINE_COMMENT_TEXT -> skip;
BLOCK_COMMENT : BLOCK_COMMENT_TEXT -> skip;

fragment LINE_COMMENT_TEXT  : '//' ~[\r\n]*;
fragment BLOCK_COMMENT_TEXT : '/*' (BLOCK_COMMENT_TEXT | .)*? '*/';

// A line break is a token rather than whitespace, because between two statements it does what a semicolon does. Within
// a statement the parser treats it as whitespace (see `statements` in FrlIrParser).
NL : LINE_BREAK;

fragment LINE_BREAK : '\r'? '\n' | '\r';

WHITESPACE : [ \t]+ -> skip;
