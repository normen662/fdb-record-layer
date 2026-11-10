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

// Definitions.
FUNC : 'func';
IN   : 'in';

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

INTEGER_LITERAL : [0-9]+;

IDENTIFIER : [a-zA-Z_] [a-zA-Z_0-9]*;

// As in Swift, any text between backticks is a name, which lets a name be spelled like a keyword or contain other
// characters: `version`, `a.b`, `first name`. The backticks are not part of the name.
ESCAPED_IDENTIFIER : '`' ~[`\r\n]+ '`';

// Comments as in Swift: a line comment runs to the end of the line, and block comments nest. A comment is whitespace;
// the line break that ends a line comment is not part of it, so it still separates statements, while a line break
// inside a block comment does not.
LINE_COMMENT  : '//' ~[\r\n]* -> skip;
BLOCK_COMMENT : '/*' (BLOCK_COMMENT | .)*? '*/' -> skip;

// A line break is a token rather than whitespace, because between two statements it does what a semicolon does. Within
// a statement the parser treats it as whitespace (see `statements` in FrlIrParser).
NL : '\r'? '\n' | '\r';

WHITESPACE : [ \t]+ -> skip;
