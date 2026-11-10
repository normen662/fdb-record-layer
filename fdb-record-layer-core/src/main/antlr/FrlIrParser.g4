/*
 * FrlIrParser.g4
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

parser grammar FrlIrParser;

options { tokenVocab=FrlIrLexer; }

//
// Line breaks
//
// The lexer turns every line break into an NL token. Between two statements a line break separates them, as a semicolon
// does (see `statements`). Within a statement a line break is whitespace wherever the statement cannot end yet, and the
// rules below allow NL* at exactly those places: a statement split across lines stays one statement. Where a statement
// could end, a line break ends it; rules for statements that can end in more than one place must therefore not allow NL*
// there. The one place within a construct where a line break is never allowed is before the `?` of a nullable type,
// which stays on the line of its type, as in Swift.
//

//
// Programs
//

// The entry point: a program is a sequence of statements, like the content of a block without the braces.
program
    : statements EOF
    ;

//
// Types
//
// FRL-IR is strictly and statically typed: there is no type promotion and there are no subtypes, so the type system has
// no placeholder types such as unknown, null, or none.
//

type
    : valueType
    | streamType
    ;

// As in Swift, a type is written by its name alone if its values are never null, and with a trailing `?` if they can be:
// `record<int, string?>?` is a nullable record which, when it is not null, holds an int and a nullable string.
valueType
    : baseType QUESTION?
    ;

baseType
    : primitiveType
    | recordType
    | arrayType
    | vectorType
    | enumType
    | functionType
    | unitType
    | parenthesizedType
    | genericType
    ;

primitiveType
    : BOOLEAN    # booleanType
    | INT        # intType
    | BIGINT     # bigintType
    | HALF       # halfType
    | FLOAT      # floatType
    | DOUBLE     # doubleType
    | STRING     # stringType
    | BYTES      # bytesType
    | UUID       # uuidType
    | VERSION    # versionType
    | VOID       # voidType
    ;

// Records are generic over any number of field types. A field may be named, and a named field may state its number in
// the stored record: `record<id(1): bigint, name: string?, int>`.
recordType
    : RECORD NL* LT NL* (recordField (NL* COMMA NL* recordField)* NL*)? GT
    ;

recordField
    : (identifier (NL* LPAREN NL* INTEGER_LITERAL NL* RPAREN)? NL* COLON NL*)? type
    ;

// `[int]` is shorthand for `array<int>`, as in Swift.
arrayType
    : ARRAY NL* LT NL* type NL* GT            # genericArrayType
    | LBRACKET NL* type NL* RBRACKET          # shorthandArrayType
    ;

// The element type, then the number of dimensions: `vector<float, 3>`, `vector<double, 512>`, `vector<half, 10>`. The
// element type determines the precision of the vector.
vectorType
    : VECTOR NL* LT NL* vectorElementType NL* COMMA NL* INTEGER_LITERAL NL* GT
    ;

vectorElementType
    : HALF
    | FLOAT
    | DOUBLE
    ;

// Every value states its number, `enum<RED = 0, GREEN = 1>`, because the numbers are part of how enum values are
// stored and need not follow the order of the values.
enumType
    : ENUM NL* LT NL* enumValue (NL* COMMA NL* enumValue)* NL* GT
    ;

enumValue
    : identifier NL* EQUAL NL* integer
    ;

// `function<int, string, bigint>` lists the parameter types followed by the result type; `(int, string) -> bigint`
// denotes the same type. As in Swift, `->` groups to the right, and a `?` after the result type belongs to the result:
// `(int) -> bigint?` returns a nullable bigint, while `((int) -> bigint)?` is a nullable function.
functionType
    : FUNCTION NL* LT NL* typeList NL* GT                        # genericFunctionType
    | LPAREN NL* (typeList NL*)? RPAREN NL* ARROW NL* type       # arrowFunctionType
    ;

typeList
    : type (NL* COMMA NL* type)*
    ;

// Parentheses only group: `(int)` is the same type as `int`.
parenthesizedType
    : LPAREN NL* type NL* RPAREN
    ;

// `()` is another spelling of `void`, as in Swift.
unitType
    : LPAREN NL* RPAREN
    ;

// A type parameter of an enclosing generic function, e.g. `T` in `func id<T>(x: T): T`. Whether a name is a type
// parameter is decided after parsing: a name that is not a type parameter in scope is not a type.
genericType
    : identifier
    ;

// A stream of values, `stream<record<id: bigint>>`, is a relation. A relation is never null, so a stream type takes no
// trailing `?`.
streamType
    : STREAM NL* LT NL* type NL* GT
    ;

integer
    : MINUS? INTEGER_LITERAL
    ;

//
// Definitions
//
// Function definitions and closures are written as in Swift, and both implement their body as a block. Unlike in Swift,
// a function definition states its result type after a colon, as a parameter states its type.
//

// `func map<T, U>(xs: [T], f: (T) -> U): [U] { }`. Without `: type`, the function returns `void`. A function definition
// ends only with its block, so it may be split across lines anywhere before that.
functionDefinition
    : FUNC NL* identifier NL* (typeParameters NL*)? LPAREN NL* (parameter (NL* COMMA NL* parameter)* NL*)? RPAREN
      (NL* COLON NL* type)? NL* block
    ;

// The type parameters of a generic function, `<T, U>`. Within the function, and within the functions nested in it, a
// type parameter is a type: `T`, `[T]`, `T?`.
typeParameters
    : LT NL* identifier (NL* COMMA NL* identifier)* NL* GT
    ;

parameter
    : identifier NL* COLON NL* type
    ;

// `{ (x: int, y: int) -> bigint in }`. As in Swift, the parameter types and the result type may be left out, to be
// inferred from where the closure is used, `{ x, y in }`, and a closure without parameters may leave out its signature
// and `in` altogether. A closure has no type parameters of its own. No statement contains a closure yet, so a program
// cannot reach this rule until expressions exist.
closure
    : LBRACE NL* (closureSignature NL* IN)? statements RBRACE
    ;

closureSignature
    : closureParameterClause (NL* ARROW NL* type)?
    ;

closureParameterClause
    : LPAREN NL* (closureParameter (NL* COMMA NL* closureParameter)* NL*)? RPAREN    # parenthesizedClosureParameters
    | identifier (NL* COMMA NL* identifier)*                                        # bareClosureParameters
    ;

closureParameter
    : identifier (NL* COLON NL* type)?
    ;

//
// Statements
//

block
    : LBRACE statements RBRACE
    ;

// Statements are separated by a line break or a semicolon, so a semicolon is needed only between statements on the same
// line. A statement needs no semicolon after it, and blank lines and repeated semicolons are allowed.
statements
    : separators? (statement (separators statement)* separators?)?
    ;

separators
    : (SEMI | NL)+
    ;

statement
    : functionDefinition
    ;

//
// Names
//

// A name is an identifier or, as in Swift, any text between backticks: `version` or `first name`. The backticks are not
// part of the name, so `ID` and ID are the same name.
identifier
    : IDENTIFIER
    | ESCAPED_IDENTIFIER
    ;
