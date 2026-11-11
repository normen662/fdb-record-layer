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
// which stays on the line of its type, as in Swift. The parameters of a closure without parentheses, `{ x, y in }`, may
// break before a `,`, `->`, or `in` although `x` alone could end a statement; none of the three can start a statement,
// so the parser still tells the parameters from the statements.
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
    : HALF      # halfVectorElementType
    | FLOAT     # floatVectorElementType
    | DOUBLE    # doubleVectorElementType
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

// `()` is another spelling of `void`, as in Swift. `void` is the result type of a function or a closure that produces no
// value, so it is a type only in that place: there is no parameter, field, element, or variable of type `void`, and no
// `void?`.
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
// a function definition states its result type after a colon, as a parameter states its type. The value of a body is the
// value of its last statement if that statement is an expression, as in a Kotlin lambda, and a body has no value
// otherwise; a trailing `;` does not change that.
//

// `func map<T, U>(xs: [T], f: (T) -> U): [U] { }`. Without `: type`, the function returns `void`. A body must end with an
// expression of the result type, unless that type is `void`, in which case a value of the last expression is discarded.
// A function definition ends only with its block, so it may be split across lines anywhere before that.
functionDefinition
    : FUNC NL* identifier NL* (typeParameters NL*)? LPAREN NL* (parameter (NL* COMMA NL* parameter)* NL*)? RPAREN
      (NL* COLON NL* type)? NL* block
    ;

// `extern func lookup(key: bigint): string?` declares a function that the runtime provides, like an external function in
// MLIR: it has a signature and no body. A declaration can end after its `)`, so the `:` of its result type stays on the
// line of the `)`.
functionDeclaration
    : EXTERN NL* FUNC NL* identifier NL* (typeParameters NL*)? LPAREN NL* (parameter (NL* COMMA NL* parameter)* NL*)?
      RPAREN (COLON NL* type)?
    ;

// The type parameters of a generic function, `<T, U>`. Within the function, and within the functions nested in it, a
// type parameter is a type: `T`, `[T]`, `T?`.
typeParameters
    : LT NL* identifier (NL* COMMA NL* identifier)* NL* GT
    ;

parameter
    : identifier NL* COLON NL* type
    ;

// `{ (x: int, y: int) -> bigint in x }`. A closure is an expression whose type is a function type, `(int, int) -> bigint`
// for this one. As in Swift, a closure may leave out the types of its parameters and its result type, `{ x, y in x }`,
// and without parameters its signature and `in` altogether, `{ 42 }`; the types it leaves out are inferred from where
// the closure is used, as in `apply({ x in x }, 1)`. A result type that neither the closure nor its context states is
// the type of the value of the body, and `void` if the body has no value. A closure has no type parameters of its own.
// The name `_` is an ordinary name.
closure
    : LBRACE (NL* closureSignature NL* IN)? statements RBRACE
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

// An expression is a statement too: `f(1)` on its own calls `f`, and as the last statement of a body it is the value of
// the body.
statement
    : functionDefinition     # definitionStatement
    | functionDeclaration    # declarationStatement
    | assignment             # assignmentStatement
    | expression             # expressionStatement
    ;

// `let x = f(1)` names the value of an expression; the type of the variable is the type of the expression. A type after
// the name, `let x: bigint = 42`, must be that type, and it is how a literal or a generic function gets a type other
// than the one it would get on its own. The variable is in scope from the next statement on.
assignment
    : LET NL* identifier (NL* COLON NL* type)? NL* EQUAL NL* expression
    ;

//
// Expressions
//
// Every expression has a type. As in Swift, a literal takes the type that its context requires: in `let x: bigint = 42`
// and in `f(42)` for `func f(x: bigint)`, `42` is a bigint. A literal without such a context has a default type: an
// integer literal is an int, a floating-point literal a double. The type arguments of a generic function are inferred
// in the same way. There is no promotion: the type of an argument is the type of its parameter, and a value of type int
// is not one of type int?.
//

expression
    : primaryExpression callSuffix*
    ;

// A name is a variable, a parameter, or a function.
primaryExpression
    : literal       # literalExpression
    | identifier    # nameExpression
    | closure       # closureExpression
    ;

// The arguments of a call. Anything of a function type can be called, `f(1)(2)` and `{ (x: int) in x }(1)` included. As
// in Swift, the `(` stays on the line of what it calls: after `let x = f`, a line break ends the statement, and since no
// statement starts with `(`, a `(` on the next line is an error.
callSuffix
    : LPAREN NL* (expression (NL* COMMA NL* expression)* NL*)? RPAREN
    ;

// An integer literal can be of any numeric type, a floating-point literal of any floating-point type, and `nil` of any
// nullable type. A literal can also be of the nullable version of such a type: in `let x: int? = 42`, the literal is a
// nullable int.
literal
    : MINUS? INTEGER_LITERAL     # integerLiteral
    | MINUS? FLOATING_LITERAL    # floatingLiteral
    | STRING_LITERAL             # stringLiteral
    | TRUE                       # trueLiteral
    | FALSE                      # falseLiteral
    | NIL                        # nilLiteral
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
