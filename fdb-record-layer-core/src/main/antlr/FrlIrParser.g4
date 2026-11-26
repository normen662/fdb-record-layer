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
// which stays on the line of its type, as in Swift. Four places allow a line break although a statement could end
// before it, because what follows cannot start a statement, so the parser still tells the statements apart:
// - the parameters of a closure without parentheses, `{ x, y in }`, may break before a `,`, `->`, or `in`;
// - a dot call or a field access may start a line, so that a chain can put one call or access on each line (see
//   `postfix`);
// - the `->` of the result type of a function declaration may start a line, as in Swift (see `functionDeclaration`);
// - the `else` of an if may stand on the line after the `}` (see `conditional`).
// The opposite holds for the `(` of a call and for a trailing closure: both stay on the line of what they call, since
// after `let x = f` a line break ends the statement. On the next line, a parenthesized expression, a record
// constructor, or a closure is a statement of its own.
//
// A line break can follow a binary operator. A binary operator can also start a line, as in Swift: if a space or a tab
// follows it, the line continues the expression of the line before, as in `x` followed by a line `    - y`. The lexer
// folds the line break into such an operator, see `CONTINUED_PLUS` in FrlIrLexer. An operator that is attached to what
// follows it starts a statement of its own, as the negation `-y` does.
//

//
// Programs
//

// The entry point: a program is a sequence of statements, like the content of a block without the braces. Its first
// statement can name the package that the program belongs to (see `packageDeclaration`).
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
    | metaType
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
    | namedType
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
    : RECORD NL* recordTypeArguments
    ;

// The fields of a record type, between angle brackets. They are also the type arguments of `record.of`, see
// `recordConstructor`.
recordTypeArguments
    : LT NL* (recordField (NL* COMMA NL* recordField)* NL*)? GT
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

// A type that has a name: a type parameter of an enclosing generic function, `T` in `func id<T>(x: T) -> T`, an extern
// type, `range` after `extern type range`, or a type alias, `row` after `typealias row = record<id: bigint>`, which
// stands for its type. FRL-IR declares the alias `stopReason` itself (see `yieldBreak`). What a name stands for is
// decided after parsing: a name that is none of these in scope is not a type.
namedType
    : identifier
    ;

// A stream of values, `stream<record<id: bigint>>`, is a relation. A relation is never null, so a stream type takes no
// trailing `?`.
streamType
    : STREAM NL* LT NL* type NL* GT
    ;

// `meta<T>` is the type of a type token, an opaque value that stands for the type `T`; `meta.of<T>()` makes one (see
// `metaConstructor`). A token fixes the type parameter of the function it is passed to: for
// `scan<T>(_ name: string, as: meta<T>) -> stream<T>`, the call `scan("restaurants", as: meta.of<restaurantType>())`
// returns a `stream<restaurantType>`. A token is never null, so a meta type takes no trailing `?`.
metaType
    : META NL* LT NL* type NL* GT
    ;

integer
    : MINUS? INTEGER_LITERAL
    ;

//
// Definitions
//
// Function definitions and closures are written as in Swift, and both implement their body as a block. As in Swift, a
// function definition states its result type after `->`, as a closure and a function type do. Also as in Swift, a body
// that is a single expression returns the value of that expression, and a trailing `;` does not change that. Any other
// body returns its values with `return` (see `returnValue`).
//

// `func map<T, U>(xs: [T], f: (T) -> U) -> [U] { }`. Without `-> type`, the function returns `void`. Unless the result
// type is `void`, the body is a single expression of the result type, or it returns a value of that type on every
// path. A body of a `void` function discards the value of a single expression. A function definition ends only with
// its block, so it may be split across lines anywhere before that. The function is in scope from the next statement
// on: FRL-IR has no recursion, so a body cannot call its own function.
functionDefinition
    : (annotation NL*)* functionSignature (NL* ARROW NL* type)? NL* block
    ;

// `extern func lookup(key: bigint) -> string?` declares a function that the runtime provides, like an external function
// in MLIR: it has a signature and no body. A declaration can end after its `)`, but the `->` of its result type may
// still start the next line, as in Swift, because no statement starts with `->`.
functionDeclaration
    : (annotation NL*)* EXTERN NL* functionSignature (NL* ARROW NL* type)?
    ;

// What a function definition and a function declaration have in common: the name, the type parameters, and the
// parameters. The result type is not part of it, so that the error for a definition such as `func f(): int { 1 }`
// still lists `->` among the expected tokens.
functionSignature
    : FUNC NL* identifier NL* (typeParameters NL*)? LPAREN NL* (parameter (NL* COMMA NL* parameter)* NL*)? RPAREN
    ;

// `overloaded func add<A, B>(a: A, b: B)` declares an overloaded function: a name with variants, each of which is a
// function of its own for concrete type arguments, see `variantDefinition`. The declaration states the type parameters
// and the parameters of all variants, with their labels. It has no body and no result type, because each variant has
// its own. It must come before its variants, which must be in the same scope.
//
// A call of the function, `add(a: x, b: y)`, calls the variant whose type arguments fit the types of the arguments and
// the type that the context requires. The variants that the call sees are those declared before it. Exactly one of them
// must fit: there is no ranking, and the default type of a literal does not count. A literal fits every variant whose
// parameter type it can have, so `1 + 2` fails as soon as two variants can take two literals. Type arguments select a
// variant explicitly: `add<int, bigint>(a: x, b: y)`. A reference to the function without a call, as in
// `fold(xs, add)`, selects a variant in the same way, from the type that its context requires.
overloadedFunctionDeclaration
    : (annotation NL*)* OVERLOADED NL* functionSignature
    ;

// `variant func add<bigint, bigint>(a: bigint, b: bigint) -> bigint { … }` defines a variant of the overloaded function
// `add`, and `extern variant func add<int, int>(a: int, b: int) -> int` declares a variant that the runtime provides.
// The type arguments are the key of the variant: one concrete type for each type parameter of the overloaded function,
// without type parameters, and no two variants of a function have the same key. The variant repeats the parameters of
// the overloaded function with its type arguments in place of the type parameters, with the same names and labels, so
// a parameter that has no type parameter in its type is the same in every variant. A variant adds its result type and
// its implementation. Because the key identifies a variant, a type parameter that no parameter uses lets the result
// type tell variants apart: after `overloaded func convert<A, R>(x: A)`, the variants `convert<int, double>` and
// `convert<int, float>` take the same argument, and the context selects one of them.
variantDefinition
    : (annotation NL*)* VARIANT NL* variantSignature (NL* ARROW NL* type)? NL* block
    ;

variantDeclaration
    : (annotation NL*)* EXTERN NL* VARIANT NL* variantSignature (NL* ARROW NL* type)?
    ;

variantSignature
    : FUNC NL* identifier NL* typeArguments NL* LPAREN NL* (parameter (NL* COMMA NL* parameter)* NL*)? RPAREN
    ;

// `extern type range` declares a type that the runtime provides, for the values that extern functions take and return.
// An extern type is a plain name without type parameters, and it can be used wherever a type can, also as `range?`.
typeDeclaration
    : (annotation NL*)* EXTERN NL* TYPE NL* identifier
    ;

// `typealias row = record<id: bigint, name: string?>` names a type, as in Swift. The alias is not a type of its own: it
// stands for its type wherever a type can be, so a value of type `row` is a value of the record type, and `row?` is the
// record type made nullable. As with `(int?)?`, a type cannot be made nullable twice, so `row?` is an error if the type
// is nullable already, and so it is if the type is a stream type. An alias is in scope from the next statement on, so
// its type cannot refer to it, and it has no type parameters. Extern types and type aliases share their names in a
// scope.
typeAlias
    : TYPEALIAS NL* identifier NL* EQUAL NL* type
    ;

// `package abc` names the package that a program belongs to. It can only be the first statement of a program, so a
// program belongs to one package at most, together with everything that the program declares. Annotations on the
// package say where the package lives outside FRL-IR, e.g. `@java("com.apple.foundationdb.record.ir") package abc`,
// so that an extern type or function needs an annotation of its own only where it lives elsewhere.
packageDeclaration
    : (annotation NL*)* PACKAGE NL* identifier
    ;

// `@java(class: "com.apple.foundationdb.record.TupleRange")` says how a declaration lives outside FRL-IR. FRL-IR keeps
// an annotation as written, a name and string arguments that may be labeled: what an annotation means is up to the
// target that binds the declaration, so a new target needs no new syntax. The one annotation that FRL-IR reads itself
// is `@operator("+")`, which makes a function the function of an operator, see `expression`. It can be on a function
// that is not a variant, also on an overloaded or a generic one, and the function must have the parameters of the
// operator: two for a binary operator, and one for a prefix operator. An annotation can stand on a line of its own
// before what it annotates, and the `(` stays on the line of the name, as for a call.
annotation
    : AT identifier (LPAREN NL* (annotationArgument (NL* COMMA NL* annotationArgument)* NL*)? RPAREN)?
    ;

annotationArgument
    : (identifier NL* COLON NL*)? STRING_LITERAL
    ;

// The type parameters of a generic function, `<T, U>`. Within the function, and within the functions nested in it, a
// type parameter is a type: `T`, `[T]`, `T?`.
typeParameters
    : LT NL* identifier (NL* COMMA NL* identifier)* NL* GT
    ;

// The type arguments of a generic or an overloaded function, one type for each type parameter: in a variant,
// `variant func add<int, int>(…)`, at a reference or a call, `id<bigint>(42)`, and at an operator, `a +<int, int> b`.
typeArguments
    : LT NL* type (NL* COMMA NL* type)* NL* GT
    ;

// A parameter of a function definition or a function declaration. As in Swift, a parameter has an argument label and a
// name: in `to x: int`, the label is `to`, and the name is `x`. A call writes the label before its argument,
// `f(to: 1)`, and the body uses the name. With one identifier, as in `x: int`, the label is the name. The label `_`
// means that the parameter has no label: after `_ x: int`, a call writes its argument alone, `f(1)`. A label in
// backticks is an ordinary label, also `_` in backticks.
parameter
    : (label=identifier NL*)? name=identifier NL* COLON NL* type
    ;

// `{ (x: int, y: int) -> bigint in x }`. A closure is an expression whose type is a function type, `(int, int) -> bigint`
// for this one. As in Swift, a closure may leave out the types of its parameters and its result type, `{ x, y in x }`,
// and without parameters its signature and `in` altogether, `{ 42 }`; the types it leaves out are inferred from where
// the closure is used, as in `apply({ x in x }, 1)`. A result type that neither the closure nor its context states is
// the type of the single expression of the body, or the type that the values of its returns agree on, and `void` if
// the body returns no value. A closure has no type parameters of its own. The name `_` is an ordinary name.
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

// An expression is a statement too: `f(1)` on its own calls `f`, and as the only statement of a body it is the value of
// the body. No statement can follow a statement that does not complete normally: a return, a yield break, an if with
// an else whose blocks both do not complete normally, or a consume whose end case does not. Such a statement would
// never run.
statement
    : functionDefinition               # functionDefinitionStatement
    | functionDeclaration              # functionDeclarationStatement
    | overloadedFunctionDeclaration    # overloadedFunctionStatement
    | variantDefinition                # variantDefinitionStatement
    | variantDeclaration               # variantDeclarationStatement
    | typeDeclaration                  # typeDeclarationStatement
    | typeAlias                        # typeAliasStatement
    | packageDeclaration               # packageStatement
    | assignment                       # assignmentStatement
    | yieldValue                       # yieldStatement
    | yieldBreak                       # yieldBreakStatement
    | returnValue                      # returnStatement
    | conditional                      # ifStatement
    | forLoop                          # forStatement
    | enumSwitch                       # switchStatement
    | consumeLoop                      # consumeStatement
    | expression                       # expressionStatement
    ;

// `let x = f(1)` names the value of an expression; the type of the variable is the type of the expression. A type after
// the name, `let x: bigint = 42`, must be that type, and it is how a literal or a generic function gets a type other
// than the one it would get on its own. The variable is in scope from the next statement on.
assignment
    : LET NL* identifier (NL* COLON NL* type)? NL* EQUAL NL* expression
    ;

// `yield x` adds `x` to the stream of the stream block it is in, as `yield return` does in C#: the block stops there
// until the next element of the stream is asked for. A yield belongs to the innermost stream block around it, and it
// must be a statement of that block itself, or of the blocks of its if, for, and consume statements, not of a function
// or a closure in the block, which can be called when the block is not running; that is the rule of C#, where a lambda
// cannot yield either.
yieldValue
    : YIELD NL* expression
    ;

// `yield break` ends the stream of the stream block it is in, as in C#: it leaves the body of the stream block, as
// `break` leaves a loop in Java, and the stream has no more elements, as when the body runs to its end. The end of a
// stream is thus a matter of control flow rather than a reason. `yield break r` stops the stream instead, for the
// reason `r`, which the `.stopped` case of a consume binds: the consumer of the stream gets the stop and can resume the
// stream from its continuation. The reason is of type `stopReason`, an alias that FRL-IR declares for the enum of the
// reasons why a cursor of the record layer stops before its end, its `NoNextReason`s other than `SOURCE_EXHAUSTED`:
// `enum<RETURN_LIMIT_REACHED = 1, TIME_LIMIT_REACHED = 2, SCAN_LIMIT_REACHED = 3, BYTE_LIMIT_REACHED = 4>`. A yield
// break can be where a yield can be. It can end its statement, so the reason stays on its line.
yieldBreak
    : YIELD NL* BREAK expression?
    ;

// `return x` leaves the function or the closure that it is in, with the value `x`, and `return` leaves one whose result
// type is `void`. The value has the result type, which is its context, as for the single expression of a body. A
// return in a closure leaves the closure, not the function around it, as in Swift. A return is in a function or a
// closure: not at the top level of a program, and not in a stream block, which `yield break` ends, as in C#. A
// function or a closure in a stream block can return. A return can end its statement, so its value stays on its line.
returnValue
    : RETURN expression?
    ;

// `if c { } else { }` runs its first block if the boolean `c` is true, and the block after `else`, if there is one,
// otherwise; `else if` adds another if. An if is a statement, not an expression, so it has no value. `else` cannot
// start a statement, so it may stand on the line after the `}`. The statements of the blocks are typed as the
// statements around the if are: in a function body or a program one by one, and in a closure or a stream block
// together with the statement that the closure or the stream block is in.
conditional
    : IF NL* conditionExpression NL* thenBlock=block (NL* ELSE NL* (elseBlock=block | elseIf=conditional))?
    ;

// `for x in xs { }` runs its block for each element `x` of the array `xs`, in order. A stream is consumed with consume
// instead, which deals with the ways in which a stream can end. The statements of the block are typed as those of the
// blocks of an if.
forLoop
    : FOR NL* identifier NL* IN NL* conditionExpression NL* block
    ;

// `consume s { case .value(x): … case .end: … case .stopped(r): … }` pulls the elements of the stream `s` in a loop,
// one case for each answer of `s`:
// - `.value(x)` runs for every element `x`, and then the loop pulls the next element;
// - `.end` runs once `s` has no more elements, and then the loop is left;
// - `.stopped(r)` runs if `s` stops before its end, for the reason `r`.
// A consume must have a `.value` and a `.stopped` case. Without an `.end` case, the loop is left at the end of `s`. The
// statements of the `.stopped` case end with a `yield break` on every path, because the reason has to be communicated
// outward: `yield break r` stops the stream block around the consume, and `yield break` ends its stream. A stop is not
// the end: `s` can be resumed from its continuation, which is part of the continuation of the stream block. A resume
// continues after the `yield break`, and the loop then pulls the next answer of `s`, as after `.value`. A consume can
// therefore be only where a yield can be. A case does not fall through to the next one, and the statements of a
// `.value` or an `.end` case may be empty.
consumeLoop
    : CONSUME NL* conditionExpression NL* LBRACE NL* consumeCase+ RBRACE
    ;

consumeCase
    : CASE NL* DOT kind=identifier (LPAREN NL* binding=identifier NL* RPAREN)? NL* COLON statements
    ;

// `switch c { case .RED, .GREEN: … case .BLUE: … default: … }` runs the statements of the case that names the value
// of `c`, which is an enum value that is not nullable, or those after `default` if no case names it. The cases name
// every value of the enum, or a `default` comes last; a `default` after cases that name every value would never run,
// so it is an error. A case names each value at most once, and no value is named in two cases. As in a consume, a case
// does not fall through to the next one, and its statements may be empty. Like an if, a switch is a statement and has
// no value, and the statements of its cases are typed as the statements around the switch. The expression in front of
// the cases takes no trailing closure (see `conditionExpression`).
enumSwitch
    : SWITCH NL* conditionExpression NL* LBRACE NL* switchCase+ defaultCase? RBRACE
    ;

switchCase
    : CASE NL* DOT identifier (NL* COMMA NL* DOT identifier)* NL* COLON statements
    ;

defaultCase
    : DEFAULT NL* COLON statements
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
// An operator is a shorter way to write a call: `a + b` calls the function that an `@operator("+")` annotation in
// scope gives to `+`, with the arguments `a` and `b` in this order and without labels. The operators bind as in Swift,
// from the strongest to the weakest:
// 1. the prefix operators `-` and `!`;
// 2. `*`, `/`, and `%`;
// 3. `+` and `-`;
// 4. the comparisons `<`, `<=`, `>`, `>=`, `==`, and `!=`.
// The operators of a level apply from left to right, so `a - b - c` is `(a - b) - c`. The comparisons do not chain:
// `a < b < c` is an error. A postfix binds more strongly than an operator, so `-x.f()` is `-(x.f())`. An operator can
// take type arguments, `a +<int, bigint> b`, which select a variant if its function is overloaded, see
// `overloadedFunctionDeclaration`.
//

expression
    : additiveExpression (comparisonOperator NL* additiveExpression)?
    ;

additiveExpression
    : multiplicativeExpression (additiveOperator NL* multiplicativeExpression)*
    ;

multiplicativeExpression
    : prefixExpression (multiplicativeOperator NL* prefixExpression)*
    ;

// The sign of a literal belongs to the literal, so `-1` is the literal, not the prefix operator applied to `1`.
prefixExpression
    : primaryExpression postfix*
    | prefixOperator prefixExpression
    ;

comparisonOperator
    : symbol=(LT | LE | GT | GE | EQUAL_EQUAL | BANG_EQUAL) typeArguments?
    ;

additiveOperator
    : symbol=(PLUS | MINUS) typeArguments?
    ;

multiplicativeOperator
    : symbol=(STAR | SLASH | PERCENT) typeArguments?
    ;

prefixOperator
    : symbol=(MINUS | BANG) typeArguments?
    ;

// A name is a variable, a parameter, or a function. The name of a generic or an overloaded function can take type
// arguments, `id<bigint>`. As in C# and Swift, `<` after a name starts type arguments if the text that follows can be
// type arguments, so `f(a < b, c > (d))` passes one argument, `a<b, c>(d)`, not two comparisons.
//
// Parentheses group an expression, `(a + b) * c`, and nothing else: `(e)` has the type of `e`, and the context of
// `(e)` is the context of `e`. So the postfixes and the operators around it apply to the value of `e`, as in
// `(x.f)(1)`, which calls a field of a function type. Parentheses are transparent for labels too: `(f)(x: 1)` needs the
// label of `f`, as in Swift. A line break may follow the `(` and precede the `)`. Within the parentheses, the line
// breaks follow the rules of other expressions, so `(a` followed by a line `    - b)` is a subtraction, and a
// condition in parentheses can take a trailing closure, `if (xs.contains { x in x == 1 }) { }`. Parentheses around
// more than one expression, or around one expression with a label, construct a record, see `recordConstructor`. `()`
// is not an expression.
primaryExpression
    : literal                                # literalExpression
    | identifier typeArguments?              # nameExpression
    | closure                                # closureExpression
    | streamBlock                            # streamExpression
    | LPAREN NL* expression NL* RPAREN       # parenthesizedExpression
    | recordConstructor                      # recordExpression
    | metaConstructor                        # metaExpression
    ;

// `stream { yield 1; yield 2 }` is a stream block, an expression of type `stream<T>` whose elements are the values that
// its yields add, in order. Its block runs as the stream is consumed, from one yield to the next, while the code around
// the stream block runs when it is reached: in `func f() -> stream<int> { let x = g(); return stream { yield x } }`,
// `g()` is called when `f` is. The element type `T` is the one that the context requires, such as the result type of
// `f`, and otherwise the type that the yielded values agree on. The block sees the names of the enclosing scopes, as a
// closure does, and it has no value: a single expression in it is a statement. `stream` cannot stand on its own, so
// the `{` may follow on the next line.
streamBlock
    : STREAM NL* block
    ;

// `(id: 1, name: "a")` constructs a record of the type `record<id: int, name: string>`. The fields have the values in
// the parentheses, in order, and the label before a value is the name of its field. A field can have no name, as in
// `(1, "a")` and `(id: 1, "a")`, and the names of a record are distinct. The label `_` needs backticks, because Swift
// reads `(_: 1, 2)` as a tuple without labels. In this short form, a record of one field needs a label, `(id: 1)`,
// because `(1)` is a parenthesized expression, and `(1,)` is not an expression. `()` is not a record either: as a
// type, `()` is `void`, which has no values. The short form states no field numbers.
//
// The long form, `record.of(id: 1, name: "a")`, constructs the same record. It also writes the records that the short
// form cannot write: `record.of(1)` has one field without a name, and `record.of()` has no fields, so its type is
// `record<>`. The long form is a generic function of any number of parameters, as the constructor `Tuple2.apply` is
// in Scala. So it can take type arguments, which are the fields, written as in a record type. They can state field
// numbers: `record.of<id(1): bigint, string>(id: 5, "a")` is a `record<id(1): bigint, string>`. With type arguments,
// the labels follow the rule of a call: a value has the name of its field as its label, and no label if its field
// has none. `record` is a keyword, but `of` is not, so `of` can still name a function or a variable. As in a call,
// the `(` stays on the line of `record.of`, while the `.` may start a line.
//
// The context of a record constructor comes first, as for a closure. It gives the values their types: in
// `let k: record<bigint, string?> = (10, nil)`, `10` is a bigint and `nil` is a nullable string. It can also make the
// record nullable, as it can make a literal nullable: the record in `let r: record<int, int>? = (1, 2)` is nullable,
// and without such a context a record is not. This holds for all forms, also with type arguments, because they state
// the fields but not the nullability. The field names must be those of the type that the context requires, so
// `(1, "a")` is not a `record<id: int, name: string>`. The line breaks are those of the arguments of a call.
recordConstructor
    : LPAREN NL* recordConstructorField (NL* COMMA NL* recordConstructorField)+ NL* RPAREN
    | LPAREN NL* label=identifier NL* COLON NL* expression NL* RPAREN
    | RECORD NL* DOT member=identifier recordTypeArguments? LPAREN NL*
        (recordConstructorField (NL* COMMA NL* recordConstructorField)* NL*)? RPAREN
    ;

recordConstructorField
    : (label=identifier NL* COLON NL*)? expression
    ;

// `meta.of<restaurantType>()` makes the type token of `restaurantType`, a value of type `meta<restaurantType>`. It is
// the only way to make a token, and a program can do nothing with a token but pass it on: the runtime gives its type to
// the external function that receives it. `meta.of` is a generic function of one type parameter and no parameters, so
// without a type argument the type comes from the context, as in `let m: meta<int> = meta.of()`. The type argument
// can be a type parameter of an enclosing function, since the runtime specializes a function before it runs it. As
// for `record.of`, `meta` is a keyword but `of` is not, the `.` may start a line, and the `(` stays on the line of
// `meta.of`.
metaConstructor
    : META NL* DOT member=identifier typeArguments? LPAREN NL* RPAREN
    ;

// The postfixes of an expression apply from left to right, each to the expression before it.
// - A call, `f(1)`. Anything of a function type can be called, `f(1)(2)` and `{ (x: int) in x }(1)` included.
// - A dot call: `x.f(1)` calls the function `f` with `x` as its first argument. The first argument has no label, also
//   if the first parameter has one. The name after the `.` is looked up among the functions only, so that a variable
//   cannot hide a function there. So `x.f(1)` is a dot call also if the record `x` has a field `f`.
// - A field access by name, `x.name`, or by ordinal, `x.0`. The ordinal is the position of the field in the record
//   type, named or not. So `x.0` is the field at position 0, and `x._0` is the field with the name `_0`. The type of
//   `x` must be a record type that is not nullable, and it must be known at the access. The lexer reads `0.1` in
//   `x.0.1` as one floating-point literal, so an access by ordinal also takes such a literal: `x.0.1` is the field at
//   position 1 of the field at position 0.
// A dot call and a field access may start a line, which continues the expression of the line before. The `(` of a
// call and a trailing closure stay on the line of what they call. The sign of a literal belongs to the literal, so
// `-1.f()` is `f(-1)`, unlike in Swift. A dot call can take type arguments, `x.f<int>(1)`.
postfix
    : callArguments                                         # callPostfix
    | NL* DOT identifier typeArguments? callArguments       # dotCallPostfix
    | NL* DOT identifier                                    # fieldPostfix
    | NL* DOT INTEGER_LITERAL                               # ordinalPostfix
    | NL* DOT FLOATING_LITERAL                              # ordinalPairPostfix
    ;

// The arguments of a call. As in Swift, a closure after the parentheses is a trailing closure, the last argument. A
// trailing closure has no label: `fold(initial: 0) { a, x in a }` is `fold(initial: 0, combine: { a, x in a })` for
// `fold(initial: int, combine: (int, int) -> int)`. Without other arguments, the parentheses can go, `run { 42 }`, also
// in a dot call, `xs.map { x in x }`. A call has one trailing closure at most. The expression in front of the block of
// an if, a for, or a consume, or in front of the cases of a switch, takes no trailing closure, see
// `conditionExpression`.
callArguments
    : arguments closure?
    | closure
    ;

// The arguments between the parentheses of a call. As in Swift, an argument has the label of its parameter, `f(x: 1)`,
// or no label if the parameter has none, `f(1)`. The arguments are in the order of the parameters. Only a function that
// the call names has labels. So a call of a function value, such as a closure or a parameter of a function type, has
// no labels.
arguments
    : LPAREN NL* (argument (NL* COMMA NL* argument)* NL*)? RPAREN
    ;

argument
    : (label=identifier NL* COLON NL*)? expression
    ;

// The expression in front of the block of an if, a for, or a consume, or in front of the cases of a switch: an
// expression whose calls take no trailing closure, as in Swift, because a `{` after it starts the block or the cases. A
// closure argument goes into the parentheses there, `if xs.contains({ x in x == 1 }) { }`. Field accesses and
// operators are the same as in other expressions, `for t in r.reviews { }` and `if !t.isEmpty() { }`, so the rules
// below repeat those of `expression`.
conditionExpression
    : conditionAdditiveExpression (comparisonOperator NL* conditionAdditiveExpression)?
    ;

conditionAdditiveExpression
    : conditionMultiplicativeExpression (additiveOperator NL* conditionMultiplicativeExpression)*
    ;

conditionMultiplicativeExpression
    : conditionPrefixExpression (multiplicativeOperator NL* conditionPrefixExpression)*
    ;

conditionPrefixExpression
    : primaryExpression conditionPostfix*
    | prefixOperator conditionPrefixExpression
    ;

conditionPostfix
    : arguments                                         # conditionCallPostfix
    | NL* DOT identifier typeArguments? arguments       # conditionDotCallPostfix
    | NL* DOT identifier                                # conditionFieldPostfix
    | NL* DOT INTEGER_LITERAL                           # conditionOrdinalPostfix
    | NL* DOT FLOATING_LITERAL                          # conditionOrdinalPairPostfix
    ;

// An integer literal can be of any numeric type, a floating-point literal of any floating-point type, and `nil` of any
// nullable type. A literal can also be of the nullable version of such a type: in `let x: int? = 42`, the literal is a
// nullable int.
//
// `.RED` is a value of an enum type, written as in Swift. Enum types have no names, and two of them can have a value of
// the same name, so the enum type comes from the context, as in `let r: stopReason = .TIME_LIMIT_REACHED` or
// `yield break .SCAN_LIMIT_REACHED`. The context must be known where the value is written, as the type of a record
// must be known where a field of it is accessed: `pair(a: c, b: .RED)` works, while in `pair(a: .RED, b: c)` the type
// is not known yet at `.RED`. A nullable context makes the value nullable, as it makes `42` nullable. A line that
// starts with a `.` continues the expression of the line before (see `postfix`), so an enum value does not start a
// statement.
literal
    : MINUS? INTEGER_LITERAL     # integerLiteral
    | MINUS? FLOATING_LITERAL    # floatingLiteral
    | STRING_LITERAL             # stringLiteral
    | TRUE                       # trueLiteral
    | FALSE                      # falseLiteral
    | NIL                        # nilLiteral
    | DOT identifier             # enumLiteral
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
