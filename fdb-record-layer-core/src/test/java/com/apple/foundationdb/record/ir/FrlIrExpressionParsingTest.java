/*
 * FrlIrExpressionParsingTest.java
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

package com.apple.foundationdb.record.ir;

import com.apple.foundationdb.half.Half;
import com.apple.foundationdb.record.ir.fragments.IArrayType;
import com.apple.foundationdb.record.ir.fragments.IAssignment;
import com.apple.foundationdb.record.ir.fragments.IBlock;
import com.apple.foundationdb.record.ir.fragments.IClosure;
import com.apple.foundationdb.record.ir.fragments.IExpression;
import com.apple.foundationdb.record.ir.fragments.IFunctionCall;
import com.apple.foundationdb.record.ir.fragments.IFunctionDefinition;
import com.apple.foundationdb.record.ir.fragments.IFunctionReference;
import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.ILiteral;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IStatement;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.fragments.IVariableReference;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.ir.FrlIrParsing.parseProgram;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for parsing FRL-IR expressions: literals, references, calls, and closures, and the inference of their types.
 * Each test program ends with an assignment or an expression, whose expression the test checks.
 */
class FrlIrExpressionParsingTest {
    private static final IType INT = new IPrimitiveType(IPrimitiveType.Kind.INT, false);
    private static final IType BIGINT = new IPrimitiveType(IPrimitiveType.Kind.BIGINT, false);
    private static final IType HALF = new IPrimitiveType(IPrimitiveType.Kind.HALF, false);
    private static final IType FLOAT = new IPrimitiveType(IPrimitiveType.Kind.FLOAT, false);
    private static final IType DOUBLE = new IPrimitiveType(IPrimitiveType.Kind.DOUBLE, false);
    private static final IType STRING = new IPrimitiveType(IPrimitiveType.Kind.STRING, false);
    private static final IType BOOLEAN = new IPrimitiveType(IPrimitiveType.Kind.BOOLEAN, false);
    private static final IType VOID = new IPrimitiveType(IPrimitiveType.Kind.VOID, false);
    private static final IBlock EMPTY_BLOCK = new IBlock(ImmutableList.of());

    private static final String ID = "extern func id<T>(x: T): T\n";
    private static final String PAIR = "extern func pair<T>(a: T, b: T)\n";
    private static final String APPLY = "extern func apply<T, U>(g: (T) -> U, x: T): U\n";

    @Nonnull
    static Stream<Arguments> literals() {
        return Stream.of(
                // without a context, a literal has its default type
                Arguments.of("let e = 42", new ILiteral(42, INT)),
                Arguments.of("let e = -42", new ILiteral(-42, INT)),
                Arguments.of("let e = 1.5", new ILiteral(1.5, DOUBLE)),
                Arguments.of("let e = 2e3", new ILiteral(2e3, DOUBLE)),
                Arguments.of("let e = -1.5e-3", new ILiteral(-1.5e-3, DOUBLE)),
                Arguments.of("let e = \"text\"", new ILiteral("text", STRING)),
                Arguments.of("let e = true", new ILiteral(true, BOOLEAN)),
                Arguments.of("let e = false", new ILiteral(false, BOOLEAN)),
                // escape sequences as in Swift
                Arguments.of("let e = \"a\\tb\\n\\\"c\\\" \\\\ \\' \\0\"", new ILiteral("a\tb\n\"c\" \\ ' \0", STRING)),
                Arguments.of("let e = \"\\u{48}\\u{1F600}\"",
                        new ILiteral("H" + new String(Character.toChars(0x1F600)), STRING)),
                // the type stated for the variable is the context of the literal
                Arguments.of("let e: bigint = 42", new ILiteral(42L, BIGINT)),
                Arguments.of("let e: double = 42", new ILiteral(42.0, DOUBLE)),
                Arguments.of("let e: float = 1.5", new ILiteral(1.5f, FLOAT)),
                Arguments.of("let e: half = 1.5", new ILiteral(Half.valueOf(1.5f), HALF)),
                Arguments.of("let e: half = 2", new ILiteral(Half.valueOf(2.0f), HALF)),
                // a literal can be of the nullable version of a type it can denote
                Arguments.of("let e: int? = 42", new ILiteral(42, INT.withNullability(true))),
                Arguments.of("let e: string? = nil", new ILiteral(null, STRING.withNullability(true))),
                Arguments.of("let e: [int]? = nil", new ILiteral(null, new IArrayType(true, INT))),
                // the ends of the ranges
                Arguments.of("let e = -2147483648", new ILiteral(Integer.MIN_VALUE, INT)),
                Arguments.of("let e: bigint = 9223372036854775807", new ILiteral(Long.MAX_VALUE, BIGINT)),
                Arguments.of("let e: half = 65504", new ILiteral(Half.MAX_VALUE, HALF)));
    }

    @ParameterizedTest
    @MethodSource("literals")
    void parseProgramLiteralCreatesLiteralOfContextType(@Nonnull final String text,
                                                         @Nonnull final IExpression expected) {
        assertThat(lastExpression(text)).isEqualTo(expected);
    }

    @Nonnull
    static Stream<Arguments> calls() {
        final IFunctionType intToInt = function(ImmutableList.of(INT), INT);
        final IFunctionType bigintToBigint = function(ImmutableList.of(BIGINT), BIGINT);
        return Stream.of(
                // a parameter is the context of its argument
                Arguments.of("extern func f(x: bigint): int\nlet e = f(42)",
                        new IFunctionCall(reference("f", function(ImmutableList.of(BIGINT), INT)),
                                ImmutableList.of(new ILiteral(42L, BIGINT)))),
                // the type arguments of a generic function are inferred from the arguments, and from the context of
                // the call
                Arguments.of(ID + "let e = id(42)",
                        new IFunctionCall(reference("id", ImmutableList.of(INT), intToInt),
                                ImmutableList.of(new ILiteral(42, INT)))),
                Arguments.of(ID + "let e: bigint = id(42)",
                        new IFunctionCall(reference("id", ImmutableList.of(BIGINT), bigintToBigint),
                                ImmutableList.of(new ILiteral(42L, BIGINT)))),
                Arguments.of(ID + "let e: bigint = id(id(42))",
                        new IFunctionCall(reference("id", ImmutableList.of(BIGINT), bigintToBigint),
                                ImmutableList.of(new IFunctionCall(reference("id", ImmutableList.of(BIGINT),
                                        bigintToBigint), ImmutableList.of(new ILiteral(42L, BIGINT)))))),
                // the order of the arguments does not matter: b makes 42 a bigint
                Arguments.of(PAIR + "let b: bigint = 7\npair(42, b)",
                        new IFunctionCall(reference("pair", ImmutableList.of(BIGINT),
                                function(ImmutableList.of(BIGINT, BIGINT), VOID)),
                                ImmutableList.of(new ILiteral(42L, BIGINT), new IVariableReference("b", BIGINT)))),
                // literals of different kinds agree on a type that they can all be
                Arguments.of(PAIR + "pair(42, 1.5)",
                        new IFunctionCall(reference("pair", ImmutableList.of(DOUBLE),
                                function(ImmutableList.of(DOUBLE, DOUBLE), VOID)),
                                ImmutableList.of(new ILiteral(42.0, DOUBLE), new ILiteral(1.5, DOUBLE)))),
                // T? matches a nullable type, and T is then its non-nullable version
                Arguments.of("extern func f<T>(x: T?): T\nlet e = f(42)",
                        new IFunctionCall(reference("f", ImmutableList.of(INT),
                                function(ImmutableList.of(INT.withNullability(true)), INT)),
                                ImmutableList.of(new ILiteral(42, INT.withNullability(true))))),
                Arguments.of("extern func f<T>(x: T?): T\nlet e: string = f(nil)",
                        new IFunctionCall(reference("f", ImmutableList.of(STRING),
                                function(ImmutableList.of(STRING.withNullability(true)), STRING)),
                                ImmutableList.of(new ILiteral(null, STRING.withNullability(true))))),
                // functions are values: an argument can be a function, generic or not, and a call can return one
                Arguments.of(APPLY + "extern func g(x: int): bigint\nlet e = apply(g, 1)",
                        new IFunctionCall(reference("apply", ImmutableList.of(INT, BIGINT),
                                function(ImmutableList.of(function(ImmutableList.of(INT), BIGINT), INT), BIGINT)),
                                ImmutableList.of(reference("g", function(ImmutableList.of(INT), BIGINT)),
                                        new ILiteral(1, INT)))),
                Arguments.of(APPLY + ID + "let e = apply(id, 1)",
                        new IFunctionCall(reference("apply", ImmutableList.of(INT, INT),
                                function(ImmutableList.of(intToInt, INT), INT)),
                                ImmutableList.of(reference("id", ImmutableList.of(INT), intToInt),
                                        new ILiteral(1, INT)))),
                Arguments.of("extern func f(x: int): (int) -> bigint\nlet e = f(1)(2)",
                        new IFunctionCall(new IFunctionCall(reference("f", function(ImmutableList.of(INT),
                                function(ImmutableList.of(INT), BIGINT))), ImmutableList.of(new ILiteral(1, INT))),
                                ImmutableList.of(new ILiteral(2, INT)))),
                // a reference to a generic function that is not called takes its type arguments from its context
                Arguments.of(ID + "let e: (int) -> int = id", reference("id", ImmutableList.of(INT), intToInt)),
                // a variable of function type, and a closure, can be called
                Arguments.of("let k = { (y: int) -> int in y }\nlet e = k(1)",
                        new IFunctionCall(new IVariableReference("k", intToInt),
                                ImmutableList.of(new ILiteral(1, INT)))),
                Arguments.of("let e = { (y: int) -> int in y }(2)",
                        new IFunctionCall(closure(ImmutableList.of(new IClosure.Parameter("y", INT)), INT,
                                new IVariableReference("y", INT)), ImmutableList.of(new ILiteral(2, INT)))),
                Arguments.of("{ (y: int) in }(2)",
                        new IFunctionCall(new IClosure(ImmutableList.of(new IClosure.Parameter("y", INT)), VOID,
                                EMPTY_BLOCK), ImmutableList.of(new ILiteral(2, INT)))),
                Arguments.of("{ }()", new IFunctionCall(new IClosure(ImmutableList.of(), VOID, EMPTY_BLOCK),
                        ImmutableList.of())),
                // a called closure takes the types of its parameters from the arguments
                Arguments.of("let e = { x -> int in x }(1)",
                        new IFunctionCall(closure(ImmutableList.of(new IClosure.Parameter("x", INT)), INT,
                                new IVariableReference("x", INT)), ImmutableList.of(new ILiteral(1, INT)))),
                // the arguments of a call can be split across lines
                Arguments.of("extern func f(x: int, y: int)\nf(\n    1,\n    2\n)",
                        new IFunctionCall(reference("f", function(ImmutableList.of(INT, INT), VOID)),
                                ImmutableList.of(new ILiteral(1, INT), new ILiteral(2, INT)))));
    }

    @ParameterizedTest
    @MethodSource("calls")
    void parseProgramCallInfersTypes(@Nonnull final String text, @Nonnull final IExpression expected) {
        assertThat(lastExpression(text)).isEqualTo(expected);
    }

    @Nonnull
    static Stream<Arguments> closures() {
        final IClosure.Parameter x = new IClosure.Parameter("x", INT);
        final IClosure.Parameter i = new IClosure.Parameter("i", INT);
        return Stream.of(
                // a closure without a signature has no parameters, and a result type of the value of its body
                Arguments.of("let e = { }", new IClosure(ImmutableList.of(), VOID, EMPTY_BLOCK)),
                Arguments.of("let a = 7\nlet e = { a }",
                        closure(ImmutableList.of(), INT, new IVariableReference("a", INT))),
                Arguments.of("let e = { () -> int in 42 }", closure(ImmutableList.of(), INT, new ILiteral(42, INT))),
                Arguments.of("let e = { (x: int, y: string) -> bigint in 7 }",
                        closure(ImmutableList.of(x, new IClosure.Parameter("y", STRING)), BIGINT,
                                new ILiteral(7L, BIGINT))),
                Arguments.of("let e = { (x: int) in }", new IClosure(ImmutableList.of(x), VOID, EMPTY_BLOCK)),
                // the body sees the parameters and the variables of the enclosing scopes
                Arguments.of("let a = 42\nlet e = { (x: int) in\n    let y = x\n    let z = a\n}",
                        new IClosure(ImmutableList.of(x), VOID, new IBlock(ImmutableList.of(
                                new IAssignment("y", new IVariableReference("x", INT)),
                                new IAssignment("z", new IVariableReference("a", INT)))))),
                // without a context, the result type is the type of the value of the body, or void without a value
                Arguments.of("let e = { (i: int) in i }",
                        closure(ImmutableList.of(i), INT, new IVariableReference("i", INT))),
                Arguments.of("let e = { (i: int) in let s = \"Hi\" }", new IClosure(ImmutableList.of(i), VOID,
                        new IBlock(ImmutableList.of(new IAssignment("s", new ILiteral("Hi", STRING)))))),
                // a stated void result discards the value of the body
                Arguments.of("let e = { (i: int) -> void in i }",
                        closure(ImmutableList.of(i), VOID, new IVariableReference("i", INT))),
                // the type stated for the variable is the context of the closure
                Arguments.of("let e: (int, int) -> void = { x, y in }",
                        new IClosure(ImmutableList.of(x, new IClosure.Parameter("y", INT)), VOID, EMPTY_BLOCK)),
                Arguments.of("let e: (int, int) -> int = { (x, y: int) in x }",
                        closure(ImmutableList.of(x, new IClosure.Parameter("y", INT)), INT,
                                new IVariableReference("x", INT))),
                Arguments.of("let e: (int) -> int = { x\n    in x }",
                        closure(ImmutableList.of(x), INT, new IVariableReference("x", INT))),
                Arguments.of("let e: (int) -> (bigint) -> int = { x in { y in x } }",
                        closure(ImmutableList.of(x), function(ImmutableList.of(BIGINT), INT),
                                closure(ImmutableList.of(new IClosure.Parameter("y", BIGINT)), INT,
                                        new IVariableReference("x", INT)))),
                // a parameter whose type is inferred can be called
                Arguments.of("let e: ((int) -> bigint) -> bigint = { f in f(1) }",
                        closure(ImmutableList.of(new IClosure.Parameter("f", function(ImmutableList.of(INT), BIGINT))),
                                BIGINT, new IFunctionCall(new IVariableReference("f",
                                        function(ImmutableList.of(INT), BIGINT)),
                                        ImmutableList.of(new ILiteral(1, INT))))),
                // the statements of a closure are inferred together with the statement that the closure is in, so a
                // later statement of the closure can still determine the type of a literal of an earlier one
                Arguments.of("extern func f(x: bigint)\nlet e = { () -> void in\n    let a = 42\n    f(a)\n}",
                        new IClosure(ImmutableList.of(), VOID, new IBlock(ImmutableList.of(
                                new IAssignment("a", new ILiteral(42L, BIGINT)),
                                new IFunctionCall(reference("f", function(ImmutableList.of(BIGINT), VOID)),
                                        ImmutableList.of(new IVariableReference("a", BIGINT))))))));
    }

    @ParameterizedTest
    @MethodSource("closures")
    void parseProgramClosureCreatesClosureOfFunctionType(@Nonnull final String text,
                                                         @Nonnull final IClosure expected) {
        final IExpression closure = lastExpression(text);
        assertThat(closure).isEqualTo(expected);
        assertThat(closure.type()).isEqualTo(new IFunctionType(false, expected.parameters()
                .stream()
                .map(IClosure.Parameter::type)
                .collect(ImmutableList.toImmutableList()), expected.resultType()));
    }

    @Nonnull
    static Stream<Arguments> closureArguments() {
        final IFunctionType intToVoid = function(ImmutableList.of(INT), VOID);
        return Stream.of(
                // the parameter of the called function is the context of the closure
                Arguments.of("extern func foo(s: string, f: (int) -> string)\nfoo(\"hello\", { i in \"Hello\" })",
                        new IFunctionCall(reference("foo", function(ImmutableList.of(STRING,
                                function(ImmutableList.of(INT), STRING)), VOID)),
                                ImmutableList.of(new ILiteral("hello", STRING),
                                        closure(ImmutableList.of(new IClosure.Parameter("i", INT)), STRING,
                                                new ILiteral("Hello", STRING))))),
                Arguments.of("extern func h(f: () -> bigint)\nh({ 42 })",
                        new IFunctionCall(reference("h", function(ImmutableList.of(function(ImmutableList.of(),
                                BIGINT)), VOID)),
                                ImmutableList.of(closure(ImmutableList.of(), BIGINT, new ILiteral(42L, BIGINT))))),
                // a later argument can determine the types of a closure
                Arguments.of(APPLY + "let e = apply({ x in x }, 1)",
                        new IFunctionCall(reference("apply", ImmutableList.of(INT, INT),
                                function(ImmutableList.of(function(ImmutableList.of(INT), INT), INT), INT)),
                                ImmutableList.of(closure(ImmutableList.of(new IClosure.Parameter("x", INT)), INT,
                                        new IVariableReference("x", INT)), new ILiteral(1, INT)))),
                // a closure that is passed for a function returning void states void to discard its value
                Arguments.of("extern func g(x: int): int\nextern func h(f: (int) -> void)\nh({ i -> void in g(i) })",
                        new IFunctionCall(reference("h", function(ImmutableList.of(intToVoid), VOID)),
                                ImmutableList.of(closure(ImmutableList.of(new IClosure.Parameter("i", INT)), VOID,
                                        new IFunctionCall(reference("g", function(ImmutableList.of(INT), INT)),
                                                ImmutableList.of(new IVariableReference("i", INT))))))),
                // a function nested in a closure can use a variable of the closure once the context has determined its
                // type
                Arguments.of("extern func h(f: (int) -> void)\nh({ x in\n    func g(): int { x }\n})",
                        new IFunctionCall(reference("h", function(ImmutableList.of(intToVoid), VOID)),
                                ImmutableList.of(new IClosure(ImmutableList.of(new IClosure.Parameter("x", INT)), VOID,
                                        new IBlock(ImmutableList.of(new IFunctionDefinition("g", ImmutableList.of(),
                                                ImmutableList.of(), INT, new IBlock(ImmutableList.of(
                                                        new IVariableReference("x", INT)))))))))));
    }

    @ParameterizedTest
    @MethodSource("closureArguments")
    void parseProgramClosureArgumentTakesTypesFromParameter(@Nonnull final String text,
                                                            @Nonnull final IExpression expected) {
        assertThat(lastExpression(text)).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // a name must be in scope
            "let e = x",
            // a literal must be of a type it can denote: nil needs a nullable type
            "let e: int = 1.5",
            "let e: string = 42",
            "let e: int = \"42\"",
            "let e: int = true",
            "let e: int = nil",
            "let e: [int] = 42",
            // a literal must be in the range of its type; without a context, an integer literal is an int
            "let e = 2147483648",
            "let e: bigint = 9223372036854775808",
            "let e: float = 1e39",
            "let e: half = 65520",
            // the type of nil, and the type arguments of a reference to a generic function, must be inferred
            "let e = nil",
            ID + "let e = id",
            ID + "let e = id(nil)",
            // there is no promotion: an int is not an int?, and a statement is solved on its own, so `a` is an int
            "extern func f(x: int?)\nfunc g(y: int) { f(y) }",
            "extern func f(x: bigint)\nlet a = 42\nf(a)",
            // also in a function body, unlike in a closure
            "extern func f(x: bigint)\nfunc g() {\n    let a = 42\n    f(a)\n}",
            // a call must match the parameters of the function
            "extern func f(x: int)\nf()",
            "extern func f(x: int)\nf(1, 2)",
            "extern func f(x: int)\nf(\"a\")",
            PAIR + "pair(42, \"a\")",
            // only a function that is not nullable can be called
            "let e = 42(1)",
            "func f(h: ((int) -> int)?) { h(1) }",
            // a type argument cannot be void where it becomes the type of a parameter
            ID + "extern func g()\nid(g())",
            // without a context, the types of the parameters of a closure cannot be inferred
            "let e = { x in }",
            "let e = { x in x }",
            "let e = { x, y in }",
            "let e = { (x, y: int) in }",
            // the closure must fit its context: the result type, the number of parameters, and void
            "extern func foo(s: string, f: (int) -> string)\nfoo(\"hello\", { i in 42 })",
            "extern func h(f: (int) -> int)\nh({ x, y in x })",
            "extern func g(x: int): int\nextern func h(f: (int) -> void)\nh({ i in g(i) })",
            // a closure whose result type is not void must have a value of that type
            "let e = { () -> int in }",
            // the type of a parameter cannot contain itself
            "let e = { f in f(f) }",
            // a function nested in a closure cannot use a variable of the closure whose type is not known yet
            "let e = { x in func g<T>(y: T): T { x } }",
            // the parameters of a closure have distinct names
            "let e: (int, int) -> void = { x, x in }",
            "let e = { x y in }",
            // a variable is not in scope in its own expression
            "let e = { (x: int) in let y = e }",
            // invalid literals: a surrogate, a code point beyond the last one, more than eight digits, an unknown
            // escape sequence, a missing closing quote, and a point without digits on both sides
            "let e = \"\\u{D800}\"",
            "let e = \"\\u{110000}\"",
            "let e = \"\\u{000000041}\"",
            "let e = \"\\q\"",
            "let e = \"open",
            "let e = 1.",
            "let e = .5",
    })
    void parseProgramInvalidExpressionFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Returns the expression of the last statement of a program, which is an assignment or an expression.
     */
    @Nonnull
    private static IExpression lastExpression(@Nonnull final String text) {
        final List<IStatement> statements = parseProgram(text).statements();
        final IStatement last = statements.get(statements.size() - 1);
        return last instanceof IAssignment assignment ? assignment.expression() : (IExpression)last;
    }

    @Nonnull
    private static IFunctionType function(@Nonnull final List<IType> parameterTypes, @Nonnull final IType resultType) {
        return new IFunctionType(false, parameterTypes, resultType);
    }

    @Nonnull
    private static IFunctionReference reference(@Nonnull final String name, @Nonnull final IFunctionType type) {
        return reference(name, ImmutableList.of(), type);
    }

    @Nonnull
    private static IFunctionReference reference(@Nonnull final String name, @Nonnull final List<IType> typeArguments,
                                                @Nonnull final IFunctionType type) {
        return new IFunctionReference(name, typeArguments, type);
    }

    /**
     * Returns a closure whose body is a single expression, which is the value of the body.
     */
    @Nonnull
    private static IClosure closure(@Nonnull final List<IClosure.Parameter> parameters, @Nonnull final IType resultType,
                                    @Nonnull final IExpression value) {
        return new IClosure(parameters, resultType, new IBlock(ImmutableList.of(value)));
    }
}
