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
import com.apple.foundationdb.record.ir.fragments.IAssignmentFragment;
import com.apple.foundationdb.record.ir.fragments.IBlockFragment;
import com.apple.foundationdb.record.ir.fragments.IClosureFragment;
import com.apple.foundationdb.record.ir.fragments.IExpressionFragment;
import com.apple.foundationdb.record.ir.fragments.IExternType;
import com.apple.foundationdb.record.ir.fragments.IFieldAccessFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionCallFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionDeclarationFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionDefinitionFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionReferenceFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.apple.foundationdb.record.ir.fragments.IIfFragment;
import com.apple.foundationdb.record.ir.fragments.ILiteralFragment;
import com.apple.foundationdb.record.ir.fragments.IMetaConstructorFragment;
import com.apple.foundationdb.record.ir.fragments.IMetaType;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IRecordConstructorFragment;
import com.apple.foundationdb.record.ir.fragments.IRecordType;
import com.apple.foundationdb.record.ir.fragments.IReturnFragment;
import com.apple.foundationdb.record.ir.fragments.IStatementFragment;
import com.apple.foundationdb.record.ir.fragments.IStreamBlockFragment;
import com.apple.foundationdb.record.ir.fragments.IStreamType;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.fragments.ITypeDeclarationFragment;
import com.apple.foundationdb.record.ir.fragments.IVariableReferenceFragment;
import com.apple.foundationdb.record.ir.fragments.IYieldFragment;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
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
 * Tests for parsing FRL-IR expressions: literals, references, calls, closures, stream blocks, parenthesized
 * expressions, and record constructors, and the inference of their types. Each test program ends with an assignment
 * or an expression, whose expression the test checks.
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
    private static final IBlockFragment EMPTY_BLOCK = new IBlockFragment(ImmutableList.of());

    private static final String ID = "extern func id<T>(x: T) -> T\n";
    private static final String PAIR = "extern func pair<T>(a: T, b: T)\n";
    private static final String APPLY = "extern func apply<T, U>(g: (T) -> U, x: T) -> U\n";
    private static final String SCAN = "extern func scan<T>(_ recordType: string, as: meta<T>) -> stream<T>\n";

    private static final IGenericType T = new IGenericType("T", false);
    private static final IGenericType U = new IGenericType("U", false);
    private static final IFunctionDeclarationFragment ID_FUNCTION =
            declaration("id", ImmutableList.of(T), ImmutableList.of(parameter("x", T)), T);
    private static final IFunctionDeclarationFragment PAIR_FUNCTION =
            declaration("pair", ImmutableList.of(T), ImmutableList.of(parameter("a", T), parameter("b", T)), VOID);
    private static final IFunctionDeclarationFragment APPLY_FUNCTION = declaration("apply", ImmutableList.of(T, U),
            ImmutableList.of(parameter("g", function(ImmutableList.of(T), U)), parameter("x", T)), U);

    @Nonnull
    static Stream<Arguments> literals() {
        return Stream.of(
                // without a context, a literal has its default type
                Arguments.of("let e = 42", new ILiteralFragment(42, INT)),
                Arguments.of("let e = -42", new ILiteralFragment(-42, INT)),
                Arguments.of("let e = 1.5", new ILiteralFragment(1.5, DOUBLE)),
                Arguments.of("let e = 2e3", new ILiteralFragment(2e3, DOUBLE)),
                Arguments.of("let e = -1.5e-3", new ILiteralFragment(-1.5e-3, DOUBLE)),
                Arguments.of("let e = \"text\"", new ILiteralFragment("text", STRING)),
                Arguments.of("let e = true", new ILiteralFragment(true, BOOLEAN)),
                Arguments.of("let e = false", new ILiteralFragment(false, BOOLEAN)),
                // escape sequences as in Swift
                Arguments.of("let e = \"a\\tb\\n\\\"c\\\" \\\\ \\' \\0\"",
                        new ILiteralFragment("a\tb\n\"c\" \\ ' \0", STRING)),
                Arguments.of("let e = \"\\u{48}\\u{1F600}\"",
                        new ILiteralFragment("H" + new String(Character.toChars(0x1F600)), STRING)),
                // the type stated for the variable is the context of the literal
                Arguments.of("let e: bigint = 42", new ILiteralFragment(42L, BIGINT)),
                Arguments.of("let e: double = 42", new ILiteralFragment(42.0, DOUBLE)),
                Arguments.of("let e: float = 1.5", new ILiteralFragment(1.5f, FLOAT)),
                Arguments.of("let e: half = 1.5", new ILiteralFragment(Half.valueOf(1.5f), HALF)),
                Arguments.of("let e: half = 2", new ILiteralFragment(Half.valueOf(2.0f), HALF)),
                // a literal can be of the nullable version of a type it can denote
                Arguments.of("let e: int? = 42", new ILiteralFragment(42, INT.withNullability(true))),
                Arguments.of("let e: string? = nil", new ILiteralFragment(null, STRING.withNullability(true))),
                Arguments.of("let e: [int]? = nil", new ILiteralFragment(null, new IArrayType(true, INT))),
                // the ends of the ranges
                Arguments.of("let e = -2147483648", new ILiteralFragment(Integer.MIN_VALUE, INT)),
                Arguments.of("let e: bigint = 9223372036854775807", new ILiteralFragment(Long.MAX_VALUE, BIGINT)),
                Arguments.of("let e: half = 65504", new ILiteralFragment(Half.MAX_VALUE, HALF)));
    }

    @ParameterizedTest
    @MethodSource("literals")
    void parseProgramLiteralCreatesLiteralOfContextType(@Nonnull final String text,
                                                         @Nonnull final IExpressionFragment expected) {
        assertThat(lastExpression(text)).isEqualTo(expected);
    }

    @Nonnull
    static Stream<Arguments> calls() {
        final IFunctionType intToInt = function(ImmutableList.of(INT), INT);
        final IFunctionType bigintToBigint = function(ImmutableList.of(BIGINT), BIGINT);
        final IGenericType nullableT = new IGenericType("T", true);
        final IFunctionDeclarationFragment fromNullable =
                declaration("f", ImmutableList.of(T), ImmutableList.of(parameter("x", nullableT)), T);
        return Stream.of(
                // a parameter is the context of its argument
                Arguments.of("extern func f(x: bigint) -> int\nlet e = f(x: 42)",
                        new IFunctionCallFragment(reference(declaration("f", ImmutableList.of(),
                                ImmutableList.of(parameter("x", BIGINT)), INT)),
                                ImmutableList.of(new ILiteralFragment(42L, BIGINT)))),
                // the type arguments of a generic function are inferred from the arguments, and from the context of
                // the call
                Arguments.of(ID + "let e = id(x: 42)",
                        new IFunctionCallFragment(reference(ID_FUNCTION, ImmutableList.of(INT), intToInt),
                                ImmutableList.of(new ILiteralFragment(42, INT)))),
                Arguments.of(ID + "let e: bigint = id(x: 42)",
                        new IFunctionCallFragment(reference(ID_FUNCTION, ImmutableList.of(BIGINT), bigintToBigint),
                                ImmutableList.of(new ILiteralFragment(42L, BIGINT)))),
                Arguments.of(ID + "let e: bigint = id(x: id(x: 42))",
                        new IFunctionCallFragment(reference(ID_FUNCTION, ImmutableList.of(BIGINT), bigintToBigint),
                                ImmutableList.of(new IFunctionCallFragment(
                                        reference(ID_FUNCTION, ImmutableList.of(BIGINT), bigintToBigint),
                                        ImmutableList.of(new ILiteralFragment(42L, BIGINT)))))),
                // a later argument can fix the type of an earlier one: b makes 42 a bigint
                Arguments.of(PAIR + "let b: bigint = 7\npair(a: 42, b: b)",
                        new IFunctionCallFragment(reference(PAIR_FUNCTION, ImmutableList.of(BIGINT),
                                function(ImmutableList.of(BIGINT, BIGINT), VOID)),
                                ImmutableList.of(new ILiteralFragment(42L, BIGINT),
                                        new IVariableReferenceFragment("b", BIGINT)))),
                // literals of different kinds agree on a type that they can all be
                Arguments.of(PAIR + "pair(a: 42, b: 1.5)",
                        new IFunctionCallFragment(reference(PAIR_FUNCTION, ImmutableList.of(DOUBLE),
                                function(ImmutableList.of(DOUBLE, DOUBLE), VOID)),
                                ImmutableList.of(new ILiteralFragment(42.0, DOUBLE),
                                        new ILiteralFragment(1.5, DOUBLE)))),
                // T? matches a nullable type, and T is then its non-nullable version
                Arguments.of("extern func f<T>(x: T?) -> T\nlet e = f(x: 42)",
                        new IFunctionCallFragment(reference(fromNullable, ImmutableList.of(INT),
                                function(ImmutableList.of(INT.withNullability(true)), INT)),
                                ImmutableList.of(new ILiteralFragment(42, INT.withNullability(true))))),
                Arguments.of("extern func f<T>(x: T?) -> T\nlet e: string = f(x: nil)",
                        new IFunctionCallFragment(reference(fromNullable, ImmutableList.of(STRING),
                                function(ImmutableList.of(STRING.withNullability(true)), STRING)),
                                ImmutableList.of(new ILiteralFragment(null, STRING.withNullability(true))))),
                // functions are values: an argument can be a function, generic or not, and a call can return one
                Arguments.of(APPLY + "extern func g(x: int) -> bigint\nlet e = apply(g: g, x: 1)",
                        new IFunctionCallFragment(reference(APPLY_FUNCTION, ImmutableList.of(INT, BIGINT),
                                function(ImmutableList.of(function(ImmutableList.of(INT), BIGINT), INT), BIGINT)),
                                ImmutableList.of(reference(declaration("g", ImmutableList.of(),
                                                ImmutableList.of(parameter("x", INT)), BIGINT)),
                                        new ILiteralFragment(1, INT)))),
                Arguments.of(APPLY + ID + "let e = apply(g: id, x: 1)",
                        new IFunctionCallFragment(reference(APPLY_FUNCTION, ImmutableList.of(INT, INT),
                                function(ImmutableList.of(intToInt, INT), INT)),
                                ImmutableList.of(reference(ID_FUNCTION, ImmutableList.of(INT), intToInt),
                                        new ILiteralFragment(1, INT)))),
                // the result of a call is a function value, whose call has no labels
                Arguments.of("extern func f(x: int) -> (int) -> bigint\nlet e = f(x: 1)(2)",
                        new IFunctionCallFragment(new IFunctionCallFragment(reference(declaration("f",
                                ImmutableList.of(), ImmutableList.of(parameter("x", INT)),
                                function(ImmutableList.of(INT), BIGINT))),
                                ImmutableList.of(new ILiteralFragment(1, INT))),
                                ImmutableList.of(new ILiteralFragment(2, INT)))),
                // a reference to a generic function that is not called takes its type arguments from its context
                Arguments.of(ID + "let e: (int) -> int = id", reference(ID_FUNCTION, ImmutableList.of(INT), intToInt)),
                // a variable of function type, and a closure, can be called
                Arguments.of("let k = { (y: int) -> int in y }\nlet e = k(1)",
                        new IFunctionCallFragment(new IVariableReferenceFragment("k", intToInt),
                                ImmutableList.of(new ILiteralFragment(1, INT)))),
                Arguments.of("let e = { (y: int) -> int in y }(2)",
                        new IFunctionCallFragment(closure(ImmutableList.of(new IClosureFragment.Parameter("y", INT)),
                                INT, new IVariableReferenceFragment("y", INT)),
                                ImmutableList.of(new ILiteralFragment(2, INT)))),
                Arguments.of("{ (y: int) in }(2)",
                        new IFunctionCallFragment(new IClosureFragment(
                                ImmutableList.of(new IClosureFragment.Parameter("y", INT)), VOID, EMPTY_BLOCK),
                                ImmutableList.of(new ILiteralFragment(2, INT)))),
                Arguments.of("{ }()", new IFunctionCallFragment(
                        new IClosureFragment(ImmutableList.of(), VOID, EMPTY_BLOCK), ImmutableList.of())),
                // a called closure takes the types of its parameters from the arguments
                Arguments.of("let e = { x -> int in x }(1)",
                        new IFunctionCallFragment(closure(ImmutableList.of(new IClosureFragment.Parameter("x", INT)),
                                INT, new IVariableReferenceFragment("x", INT)),
                                ImmutableList.of(new ILiteralFragment(1, INT)))),
                // the arguments of a call can be split across lines
                Arguments.of("extern func f(x: int, y: int)\nf(\n    x: 1,\n    y: 2\n)",
                        new IFunctionCallFragment(reference(declaration("f", ImmutableList.of(),
                                ImmutableList.of(parameter("x", INT), parameter("y", INT)), VOID)),
                                ImmutableList.of(new ILiteralFragment(1, INT), new ILiteralFragment(2, INT)))));
    }

    @ParameterizedTest
    @MethodSource("calls")
    void parseProgramCallInfersTypes(@Nonnull final String text, @Nonnull final IExpressionFragment expected) {
        assertThat(lastExpression(text)).isEqualTo(expected);
    }

    @Nonnull
    static Stream<Arguments> closures() {
        final IClosureFragment.Parameter x = new IClosureFragment.Parameter("x", INT);
        final IClosureFragment.Parameter i = new IClosureFragment.Parameter("i", INT);
        final IClosureFragment.Parameter c = new IClosureFragment.Parameter("c", BOOLEAN);
        final IVariableReferenceFragment cReference = new IVariableReferenceFragment("c", BOOLEAN);
        final IClosureFragment returnsOneOrTwo = new IClosureFragment(ImmutableList.of(c), INT,
                new IBlockFragment(ImmutableList.of(new IIfFragment(cReference, new IBlockFragment(ImmutableList.of(
                        new IReturnFragment(new ILiteralFragment(1, INT)))), null),
                        new IReturnFragment(new ILiteralFragment(2, INT)))));
        return Stream.of(
                // a closure without a signature has no parameters, and a result type of the value of its body
                Arguments.of("let e = { }", new IClosureFragment(ImmutableList.of(), VOID, EMPTY_BLOCK)),
                Arguments.of("let a = 7\nlet e = { a }",
                        closure(ImmutableList.of(), INT, new IVariableReferenceFragment("a", INT))),
                Arguments.of("let e = { () -> int in 42 }",
                        closure(ImmutableList.of(), INT, new ILiteralFragment(42, INT))),
                Arguments.of("let e = { (x: int, y: string) -> bigint in 7 }",
                        closure(ImmutableList.of(x, new IClosureFragment.Parameter("y", STRING)), BIGINT,
                                new ILiteralFragment(7L, BIGINT))),
                Arguments.of("let e = { (x: int) in }", new IClosureFragment(ImmutableList.of(x), VOID, EMPTY_BLOCK)),
                // the body sees the parameters and the variables of the enclosing scopes
                Arguments.of("let a = 42\nlet e = { (x: int) in\n    let y = x\n    let z = a\n}",
                        new IClosureFragment(ImmutableList.of(x), VOID, new IBlockFragment(ImmutableList.of(
                                new IAssignmentFragment("y", new IVariableReferenceFragment("x", INT)),
                                new IAssignmentFragment("z", new IVariableReferenceFragment("a", INT)))))),
                // without a context, the result type is the type of the value of the body, or void without a value
                Arguments.of("let e = { (i: int) in i }",
                        closure(ImmutableList.of(i), INT, new IVariableReferenceFragment("i", INT))),
                Arguments.of("let e = { (i: int) in let s = \"Hi\" }", new IClosureFragment(ImmutableList.of(i), VOID,
                        new IBlockFragment(ImmutableList.of(
                                new IAssignmentFragment("s", new ILiteralFragment("Hi", STRING)))))),
                // a stated void result discards the value of the body
                Arguments.of("let e = { (i: int) -> void in i }",
                        closure(ImmutableList.of(i), VOID, new IVariableReferenceFragment("i", INT))),
                // the type stated for the variable is the context of the closure
                Arguments.of("let e: (int, int) -> void = { x, y in }",
                        new IClosureFragment(ImmutableList.of(x, new IClosureFragment.Parameter("y", INT)), VOID,
                                EMPTY_BLOCK)),
                Arguments.of("let e: (int, int) -> int = { (x, y: int) in x }",
                        closure(ImmutableList.of(x, new IClosureFragment.Parameter("y", INT)), INT,
                                new IVariableReferenceFragment("x", INT))),
                Arguments.of("let e: (int) -> int = { x\n    in x }",
                        closure(ImmutableList.of(x), INT, new IVariableReferenceFragment("x", INT))),
                Arguments.of("let e: (int) -> (bigint) -> int = { x in { y in x } }",
                        closure(ImmutableList.of(x), function(ImmutableList.of(BIGINT), INT),
                                closure(ImmutableList.of(new IClosureFragment.Parameter("y", BIGINT)), INT,
                                        new IVariableReferenceFragment("x", INT)))),
                // a parameter whose type is inferred can be called
                Arguments.of("let e: ((int) -> bigint) -> bigint = { f in f(1) }",
                        closure(ImmutableList.of(
                                        new IClosureFragment.Parameter("f", function(ImmutableList.of(INT), BIGINT))),
                                BIGINT, new IFunctionCallFragment(new IVariableReferenceFragment("f",
                                        function(ImmutableList.of(INT), BIGINT)),
                                        ImmutableList.of(new ILiteralFragment(1, INT))))),
                // the statements of a closure are inferred together with the statement that the closure is in, so a
                // later statement of the closure can still determine the type of a literal of an earlier one
                Arguments.of("extern func f(x: bigint)\nlet e = { () -> void in\n    let a = 42\n    f(x: a)\n}",
                        new IClosureFragment(ImmutableList.of(), VOID, new IBlockFragment(ImmutableList.of(
                                new IAssignmentFragment("a", new ILiteralFragment(42L, BIGINT)),
                                new IFunctionCallFragment(reference(declaration("f", ImmutableList.of(),
                                                ImmutableList.of(parameter("x", BIGINT)), VOID)),
                                        ImmutableList.of(new IVariableReferenceFragment("a", BIGINT))))))),
                // a return leaves the closure; without a stated result type, the values of the returns give it
                Arguments.of("let e = { (c: boolean) -> int in\n    if c { return 1 }\n    return 2\n}",
                        returnsOneOrTwo),
                Arguments.of("let e = { (c: boolean) in\n    if c { return 1 }\n    return 2\n}", returnsOneOrTwo),
                // as in Swift, a body of more than one statement without a return of a value returns void
                Arguments.of("let e = { (i: int) in\n    let j = i\n    j\n}",
                        new IClosureFragment(ImmutableList.of(i), VOID, new IBlockFragment(ImmutableList.of(
                                new IAssignmentFragment("j", new IVariableReferenceFragment("i", INT)),
                                new IVariableReferenceFragment("j", INT))))),
                Arguments.of("let e = { (c: boolean) in\n    if c { return }\n    return\n}",
                        new IClosureFragment(ImmutableList.of(c), VOID, new IBlockFragment(ImmutableList.of(
                                new IIfFragment(cReference, new IBlockFragment(ImmutableList.of(
                                        new IReturnFragment(null))), null), new IReturnFragment(null))))),
                // the context gives the result type, and so the type of the value of a return
                Arguments.of("let e: (int) -> bigint = { x in\n    let y = x\n    return 42\n}",
                        new IClosureFragment(ImmutableList.of(x), BIGINT, new IBlockFragment(ImmutableList.of(
                                new IAssignmentFragment("y", new IVariableReferenceFragment("x", INT)),
                                new IReturnFragment(new ILiteralFragment(42L, BIGINT)))))));
    }

    @ParameterizedTest
    @MethodSource("closures")
    void parseProgramClosureCreatesClosureOfFunctionType(@Nonnull final String text,
                                                         @Nonnull final IClosureFragment expected) {
        final IExpressionFragment closure = lastExpression(text);
        assertThat(closure).isEqualTo(expected);
        assertThat(closure.type()).isEqualTo(new IFunctionType(false, expected.parameters()
                .stream()
                .map(IClosureFragment.Parameter::type)
                .collect(ImmutableList.toImmutableList()), expected.resultType()));
    }

    @Nonnull
    static Stream<Arguments> closureArguments() {
        final IFunctionType intToVoid = function(ImmutableList.of(INT), VOID);
        final IFunctionDeclarationFragment hOfIntToVoid =
                declaration("h", ImmutableList.of(), ImmutableList.of(parameter("f", intToVoid)), VOID);
        return Stream.of(
                // the parameter of the called function is the context of the closure
                Arguments.of("extern func foo(s: string, f: (int) -> string)\n"
                                + "foo(s: \"hello\", f: { i in \"Hello\" })",
                        new IFunctionCallFragment(reference(declaration("foo", ImmutableList.of(),
                                ImmutableList.of(parameter("s", STRING),
                                        parameter("f", function(ImmutableList.of(INT), STRING))), VOID)),
                                ImmutableList.of(new ILiteralFragment("hello", STRING),
                                        closure(ImmutableList.of(new IClosureFragment.Parameter("i", INT)), STRING,
                                                new ILiteralFragment("Hello", STRING))))),
                Arguments.of("extern func h(f: () -> bigint)\nh(f: { 42 })",
                        new IFunctionCallFragment(reference(declaration("h", ImmutableList.of(),
                                ImmutableList.of(parameter("f", function(ImmutableList.of(), BIGINT))), VOID)),
                                ImmutableList.of(
                                        closure(ImmutableList.of(), BIGINT, new ILiteralFragment(42L, BIGINT))))),
                // a later argument can determine the types of a closure
                Arguments.of(APPLY + "let e = apply(g: { x in x }, x: 1)",
                        new IFunctionCallFragment(reference(APPLY_FUNCTION, ImmutableList.of(INT, INT),
                                function(ImmutableList.of(function(ImmutableList.of(INT), INT), INT), INT)),
                                ImmutableList.of(closure(ImmutableList.of(new IClosureFragment.Parameter("x", INT)),
                                        INT, new IVariableReferenceFragment("x", INT)), new ILiteralFragment(1, INT)))),
                // a closure that is passed for a function returning void states void to discard its value
                Arguments.of("extern func g(x: int) -> int\nextern func h(f: (int) -> void)\n"
                                + "h(f: { i -> void in g(x: i) })",
                        new IFunctionCallFragment(reference(hOfIntToVoid),
                                ImmutableList.of(closure(ImmutableList.of(new IClosureFragment.Parameter("i", INT)),
                                        VOID, new IFunctionCallFragment(reference(declaration("g", ImmutableList.of(),
                                                        ImmutableList.of(parameter("x", INT)), INT)),
                                                ImmutableList.of(new IVariableReferenceFragment("i", INT))))))),
                // a function nested in a closure can use a variable of the closure once the context has determined its
                // type
                Arguments.of("extern func h(f: (int) -> void)\nh(f: { x in\n    func g() -> int { x }\n})",
                        new IFunctionCallFragment(reference(hOfIntToVoid),
                                ImmutableList.of(new IClosureFragment(
                                        ImmutableList.of(new IClosureFragment.Parameter("x", INT)), VOID,
                                        new IBlockFragment(ImmutableList.of(new IFunctionDefinitionFragment("g",
                                                ImmutableList.of(), ImmutableList.of(), INT,
                                                new IBlockFragment(ImmutableList.of(
                                                        new IVariableReferenceFragment("x", INT)))))))))));
    }

    @ParameterizedTest
    @MethodSource("closureArguments")
    void parseProgramClosureArgumentTakesTypesFromParameter(@Nonnull final String text,
                                                            @Nonnull final IExpressionFragment expected) {
        assertThat(lastExpression(text)).isEqualTo(expected);
    }

    @Nonnull
    static Stream<Arguments> streams() {
        final IType nullableString = STRING.withNullability(true);
        return Stream.of(
                // a stream block is a stream of the values that it yields, in order; without a context, they determine
                // its element type
                Arguments.of("let e = stream { yield 1; yield 2 }",
                        stream(INT, ImmutableList.of(new ILiteralFragment(1, INT), new ILiteralFragment(2, INT)))),
                // the context determines the element type, which is the context of every yielded value
                Arguments.of("let e: stream<bigint> = stream { yield 1 }",
                        stream(BIGINT, ImmutableList.of(new ILiteralFragment(1L, BIGINT)))),
                Arguments.of("let e: stream<string?> = stream { yield nil; yield \"a\" }", stream(nullableString,
                        ImmutableList.of(new ILiteralFragment(null, nullableString),
                                new ILiteralFragment("a", nullableString)))),
                Arguments.of("let e: stream<(int) -> int> = stream { yield { x in x } }",
                        stream(function(ImmutableList.of(INT), INT), ImmutableList.of(closure(
                                ImmutableList.of(new IClosureFragment.Parameter("x", INT)), INT,
                                new IVariableReferenceFragment("x", INT))))),
                // without yields, a stream block is an empty stream
                Arguments.of("let e: stream<int> = stream { }", new IStreamBlockFragment(INT, EMPTY_BLOCK)),
                // the block sees the variables of the enclosing scopes, and the value of its last expression is
                // discarded
                Arguments.of("let a = 7\nlet e = stream {\n    let b = a\n    yield b\n    \"done\"\n}",
                        new IStreamBlockFragment(INT, new IBlockFragment(ImmutableList.of(
                                new IAssignmentFragment("b", new IVariableReferenceFragment("a", INT)),
                                new IYieldFragment(new IVariableReferenceFragment("b", INT)),
                                new ILiteralFragment("done", STRING))))),
                // the statements of a stream block are inferred together with the statement that the stream block is
                // in, as those of a closure are
                Arguments.of("extern func f(x: bigint)\nlet e = stream {\n    let a = 42\n    f(x: a)\n    yield a\n}",
                        new IStreamBlockFragment(BIGINT, new IBlockFragment(ImmutableList.of(
                                new IAssignmentFragment("a", new ILiteralFragment(42L, BIGINT)),
                                new IFunctionCallFragment(reference(declaration("f", ImmutableList.of(),
                                                ImmutableList.of(parameter("x", BIGINT)), VOID)),
                                        ImmutableList.of(new IVariableReferenceFragment("a", BIGINT))),
                                new IYieldFragment(new IVariableReferenceFragment("a", BIGINT)))))),
                // a yield belongs to the innermost stream block
                Arguments.of("let e = stream { yield stream { yield 1 } }", stream(new IStreamType(INT),
                        ImmutableList.of(stream(INT, ImmutableList.of(new ILiteralFragment(1, INT)))))),
                // a stream block is an argument like any other: the parameter is its context
                Arguments.of("extern func count<T>(s: stream<T>) -> bigint\nlet e = count(s: stream { yield \"a\" })",
                        new IFunctionCallFragment(reference(declaration("count", ImmutableList.of(T),
                                        ImmutableList.of(parameter("s", new IStreamType(T))), BIGINT),
                                ImmutableList.of(STRING), function(ImmutableList.of(new IStreamType(STRING)), BIGINT)),
                                ImmutableList.of(stream(STRING, ImmutableList.of(new ILiteralFragment("a", STRING)))))),
                // a closure can return a stream block, whose yields add to the stream, not to the closure
                Arguments.of("let e = { (x: int) in stream { yield x } }", closure(ImmutableList.of(
                                new IClosureFragment.Parameter("x", INT)), new IStreamType(INT),
                        stream(INT, ImmutableList.of(new IVariableReferenceFragment("x", INT))))),
                // `stream` cannot stand on its own, so its block may start on the next line
                Arguments.of("let e = stream\n{ yield 1 }",
                        stream(INT, ImmutableList.of(new ILiteralFragment(1, INT)))));
    }

    @ParameterizedTest
    @MethodSource("streams")
    void parseProgramStreamBlockCreatesStreamOfYieldedValues(@Nonnull final String text,
                                                              @Nonnull final IExpressionFragment expected) {
        assertThat(lastExpression(text)).isEqualTo(expected);
    }

    @Nonnull
    static Stream<Arguments> dotCallsAndTrailingClosures() {
        final String inc = "extern func inc(x: int) -> int\n";
        final IFunctionDeclarationFragment incFunction =
                declaration("inc", ImmutableList.of(), ImmutableList.of(parameter("x", INT)), INT);
        final IFunctionCallFragment incOfIncOfOne = new IFunctionCallFragment(reference(incFunction), ImmutableList.of(
                new IFunctionCallFragment(reference(incFunction), ImmutableList.of(new ILiteralFragment(1, INT)))));
        final IFunctionType intToInt = function(ImmutableList.of(INT), INT);
        final IFunctionDeclarationFragment fold = declaration("fold", ImmutableList.of(),
                ImmutableList.of(parameter("x", INT), parameter("f", intToInt)), INT);
        final IClosureFragment.Parameter y = new IClosureFragment.Parameter("y", INT);
        return Stream.of(
                // a dot call calls the function with the expression before the dot as its first argument, which has no
                // label; the other arguments have the labels of their parameters
                Arguments.of(inc + "let e = 1.inc()",
                        new IFunctionCallFragment(reference(incFunction),
                                ImmutableList.of(new ILiteralFragment(1, INT)))),
                Arguments.of(inc + "let e = 1.inc().inc()", incOfIncOfOne),
                Arguments.of("extern func f(x: int, y: bigint) -> string\nlet e = 1.f(y: 2)",
                        new IFunctionCallFragment(reference(declaration("f", ImmutableList.of(),
                                ImmutableList.of(parameter("x", INT), parameter("y", BIGINT)), STRING)),
                                ImmutableList.of(new ILiteralFragment(1, INT), new ILiteralFragment(2L, BIGINT)))),
                // the context of the call reaches the first argument through the type parameter
                Arguments.of(ID + "let e: bigint = 42.id()",
                        new IFunctionCallFragment(reference(ID_FUNCTION, ImmutableList.of(BIGINT),
                                function(ImmutableList.of(BIGINT), BIGINT)),
                                ImmutableList.of(new ILiteralFragment(42L, BIGINT)))),
                // a dot call can start a line, also after a blank line or a comment
                Arguments.of(inc + "let e = 1\n    .inc()\n\n    // and once more\n    .inc()", incOfIncOfOne),
                // the sign belongs to the literal, and a floating-point literal keeps its point
                Arguments.of(inc + "let e = -1.inc()",
                        new IFunctionCallFragment(reference(incFunction),
                                ImmutableList.of(new ILiteralFragment(-1, INT)))),
                Arguments.of("extern func floor(x: double) -> int\nlet e = 1.5.floor()",
                        new IFunctionCallFragment(reference(declaration("floor", ImmutableList.of(),
                                ImmutableList.of(parameter("x", DOUBLE)), INT)),
                                ImmutableList.of(new ILiteralFragment(1.5, DOUBLE)))),
                // a trailing closure is the last argument, without a label, and without other arguments the parentheses
                // can go
                Arguments.of("extern func fold(x: int, f: (int) -> int) -> int\nlet e = fold(x: 1) { y in y }",
                        new IFunctionCallFragment(reference(fold), ImmutableList.of(new ILiteralFragment(1, INT),
                                closure(ImmutableList.of(y), INT, new IVariableReferenceFragment("y", INT))))),
                Arguments.of("extern func run(f: () -> int) -> int\nlet e = run { 42 }",
                        new IFunctionCallFragment(reference(declaration("run", ImmutableList.of(),
                                ImmutableList.of(parameter("f", function(ImmutableList.of(), INT))), INT)),
                                ImmutableList.of(closure(ImmutableList.of(), INT, new ILiteralFragment(42, INT))))),
                Arguments.of("extern func fold(x: int, f: (int) -> int) -> int\n"
                                + "let e = fold(x: fold(x: 1) { y in y }) { z in z }",
                        new IFunctionCallFragment(reference(fold), ImmutableList.of(
                                new IFunctionCallFragment(reference(fold), ImmutableList.of(
                                        new ILiteralFragment(1, INT),
                                        closure(ImmutableList.of(y), INT, new IVariableReferenceFragment("y", INT)))),
                                closure(ImmutableList.of(new IClosureFragment.Parameter("z", INT)), INT,
                                        new IVariableReferenceFragment("z", INT))))),
                // a dot call can take arguments and a trailing closure
                Arguments.of("extern func combine(x: int, y: int, f: (int, int) -> int) -> int\n"
                                + "let e = 1.combine(y: 2) { a, b in a }",
                        new IFunctionCallFragment(reference(declaration("combine", ImmutableList.of(),
                                ImmutableList.of(parameter("x", INT), parameter("y", INT),
                                        parameter("f", function(ImmutableList.of(INT, INT), INT))), INT)),
                                ImmutableList.of(new ILiteralFragment(1, INT), new ILiteralFragment(2, INT),
                                        closure(ImmutableList.of(new IClosureFragment.Parameter("a", INT),
                                                new IClosureFragment.Parameter("b", INT)), INT,
                                                new IVariableReferenceFragment("a", INT))))),
                // the trailing closure of a generic function takes its types from the other arguments
                Arguments.of("extern func map<T, U>(x: T, f: (T) -> U) -> U\nlet e = 1.map { x in x }",
                        new IFunctionCallFragment(reference(declaration("map", ImmutableList.of(T, U),
                                        ImmutableList.of(parameter("x", T),
                                                parameter("f", function(ImmutableList.of(T), U))), U),
                                ImmutableList.of(INT, INT), function(ImmutableList.of(INT, intToInt), INT)),
                                ImmutableList.of(new ILiteralFragment(1, INT), closure(ImmutableList.of(
                                        new IClosureFragment.Parameter("x", INT)), INT,
                                        new IVariableReferenceFragment("x", INT))))),
                // anything of a function type takes a trailing closure
                Arguments.of("let k = { (f: () -> int) -> int in f() }\nlet e = k { 1 }",
                        new IFunctionCallFragment(new IVariableReferenceFragment("k", function(ImmutableList.of(
                                function(ImmutableList.of(), INT)), INT)),
                                ImmutableList.of(closure(ImmutableList.of(), INT, new ILiteralFragment(1, INT))))),
                // a dot call looks up functions only, so a variable of the name does not hide the function
                Arguments.of("extern func next(x: int) -> int\nlet e = { (next: int) in next.next() }",
                        closure(ImmutableList.of(new IClosureFragment.Parameter("next", INT)), INT,
                                new IFunctionCallFragment(reference(declaration("next", ImmutableList.of(),
                                        ImmutableList.of(parameter("x", INT)), INT)),
                                        ImmutableList.of(new IVariableReferenceFragment("next", INT))))));
    }

    @ParameterizedTest
    @MethodSource("dotCallsAndTrailingClosures")
    void parseProgramDotCallOrTrailingClosureIsCall(@Nonnull final String text,
                                                    @Nonnull final IExpressionFragment expected) {
        assertThat(lastExpression(text)).isEqualTo(expected);
    }

    @Test
    void parseProgramDotCallChainTypesLeftToRight() {
        final String text = """
                extern type row
                extern func scan() -> stream<row>
                extern func filter<T>(upstream: stream<T>, predicate: (T) -> boolean) -> stream<T>
                extern func map<T, R>(upstream: stream<T>, result: (T) -> R) -> stream<R>
                extern func isGood(r: row) -> boolean
                extern func score(r: row) -> int
                let e = scan()
                    .filter { r in r.isGood() }
                    .map { r in r.score() }""";
        final IType row = new IExternType(new ITypeDeclarationFragment("row", ImmutableList.of(), null), false);
        final IGenericType r = new IGenericType("R", false);
        final IFunctionDeclarationFragment scan =
                declaration("scan", ImmutableList.of(), ImmutableList.of(), new IStreamType(row));
        final IFunctionDeclarationFragment filter = declaration("filter", ImmutableList.of(T),
                ImmutableList.of(parameter("upstream", new IStreamType(T)),
                        parameter("predicate", function(ImmutableList.of(T), BOOLEAN))), new IStreamType(T));
        final IFunctionDeclarationFragment map = declaration("map", ImmutableList.of(T, r),
                ImmutableList.of(parameter("upstream", new IStreamType(T)),
                        parameter("result", function(ImmutableList.of(T), r))), new IStreamType(r));
        final IFunctionDeclarationFragment isGood =
                declaration("isGood", ImmutableList.of(), ImmutableList.of(parameter("r", row)), BOOLEAN);
        final IFunctionDeclarationFragment score =
                declaration("score", ImmutableList.of(), ImmutableList.of(parameter("r", row)), INT);
        final IClosureFragment.Parameter rowParameter = new IClosureFragment.Parameter("r", row);
        final IVariableReferenceFragment rowReference = new IVariableReferenceFragment("r", row);
        final IFunctionCallFragment filtered = new IFunctionCallFragment(reference(filter, ImmutableList.of(row),
                function(ImmutableList.of(new IStreamType(row), function(ImmutableList.of(row), BOOLEAN)),
                        new IStreamType(row))),
                ImmutableList.of(new IFunctionCallFragment(reference(scan), ImmutableList.of()),
                        closure(ImmutableList.of(rowParameter), BOOLEAN,
                                new IFunctionCallFragment(reference(isGood), ImmutableList.of(rowReference)))));
        // each call has the type of its row as the context of its closure, so `r.isGood()` and `r.score()` resolve
        assertThat(lastExpression(text)).isEqualTo(new IFunctionCallFragment(reference(map, ImmutableList.of(row, INT),
                function(ImmutableList.of(new IStreamType(row), function(ImmutableList.of(row), INT)),
                        new IStreamType(INT))),
                ImmutableList.of(filtered, closure(ImmutableList.of(rowParameter), INT,
                        new IFunctionCallFragment(reference(score), ImmutableList.of(rowReference))))));
    }

    @Nonnull
    static Stream<Arguments> fieldAccesses() {
        final String r = "extern func r() -> record<id: bigint, name: string?>\n";
        final IRecordType row = new IRecordType(false, ImmutableList.of(IRecordType.Field.of(BIGINT, "id"),
                IRecordType.Field.of(STRING.withNullability(true), "name")));
        final IFunctionCallFragment callR = new IFunctionCallFragment(
                reference(declaration("r", ImmutableList.of(), ImmutableList.of(), row)), ImmutableList.of());
        final String q = "extern func q() -> record<a: int, _0: string>\n";
        final IFunctionCallFragment callQ = new IFunctionCallFragment(reference(declaration("q", ImmutableList.of(),
                ImmutableList.of(), new IRecordType(false, ImmutableList.of(IRecordType.Field.of(INT, "a"),
                        IRecordType.Field.of(STRING, "_0"))))), ImmutableList.of());
        final IFunctionCallFragment callN = new IFunctionCallFragment(reference(declaration("n", ImmutableList.of(),
                ImmutableList.of(), new IRecordType(false, ImmutableList.of(IRecordType.Field.unnamedOf(INT),
                        IRecordType.Field.unnamedOf(new IRecordType(false, ImmutableList.of(
                                IRecordType.Field.unnamedOf(BIGINT), IRecordType.Field.unnamedOf(STRING)))))))),
                ImmutableList.of());
        final IRecordType box = new IRecordType(false, ImmutableList.of(IRecordType.Field.of(T, "value")));
        final IRecordType boxOfBigint =
                new IRecordType(false, ImmutableList.of(IRecordType.Field.of(BIGINT, "value")));
        final IFunctionDeclarationFragment boxFunction = declaration("box", ImmutableList.of(T),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter(null, "x", T)), box);
        final IFunctionDeclarationFragment withFunction = declaration("with", ImmutableList.of(T, U),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter(null, "x", T),
                        parameter("f", function(ImmutableList.of(T), U))), U);
        final IClosureFragment idOfV = closure(ImmutableList.of(new IClosureFragment.Parameter("v", row)), BIGINT,
                new IFieldAccessFragment(new IVariableReferenceFragment("v", row), 0));
        final IFunctionDeclarationFragment nameFunction = declaration("name", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter(null, "x", row)), INT);
        return Stream.of(
                // an access by name gives the ordinal of the field, so it equals the access by that ordinal
                Arguments.of(r + "let e = r().name", new IFieldAccessFragment(callR, 1)),
                Arguments.of(r + "let e = r().1", new IFieldAccessFragment(callR, 1)),
                Arguments.of(r + "let e = r().0", new IFieldAccessFragment(callR, 0)),
                // an ordinal is a position, also of a named field: the field with the name _0 is at ordinal 1
                Arguments.of(q + "let e = q()._0", new IFieldAccessFragment(callQ, 1)),
                Arguments.of(q + "let e = q().0", new IFieldAccessFragment(callQ, 0)),
                // fields without names; the lexer reads 1.0 as one literal, which stands for two ordinals
                Arguments.of("extern func n() -> record<int, record<bigint, string>>\nlet e = n().1.0",
                        new IFieldAccessFragment(new IFieldAccessFragment(callN, 1), 0)),
                Arguments.of("extern func n() -> record<int, record<bigint, string>>\nlet e = n().1\n    .1",
                        new IFieldAccessFragment(new IFieldAccessFragment(callN, 1), 1)),
                // a field access may start a line, and the context of the expression is the context of the access
                Arguments.of(r + "let e: string? = r()\n    .name", new IFieldAccessFragment(callR, 1)),
                // the type of a field can still contain variables: the context makes 42 a bigint
                Arguments.of("extern func box<T>(_ x: T) -> record<value: T>\nlet e: bigint = box(42).value",
                        new IFieldAccessFragment(new IFunctionCallFragment(reference(boxFunction,
                                ImmutableList.of(BIGINT), function(ImmutableList.of(BIGINT), boxOfBigint)),
                                ImmutableList.of(new ILiteralFragment(42L, BIGINT))), 0)),
                // a closure parameter has its type from the call before the visitor reaches the body of the closure
                Arguments.of(r + "extern func with<T, U>(_ x: T, f: (T) -> U) -> U\nlet e = with(r()) { v in v.id }",
                        new IFunctionCallFragment(reference(withFunction, ImmutableList.of(row, BIGINT),
                                function(ImmutableList.of(row, function(ImmutableList.of(row), BIGINT)), BIGINT)),
                                ImmutableList.of(callR, idOfV))),
                // with arguments, a dot is a dot call of a function, also if the record has a field of the name
                Arguments.of(r + "extern func name(_ x: record<id: bigint, name: string?>) -> int\nlet e = r().name()",
                        new IFunctionCallFragment(reference(nameFunction), ImmutableList.of(callR))));
    }

    @ParameterizedTest
    @MethodSource("fieldAccesses")
    void parseProgramFieldAccessReadsFieldAtOrdinal(@Nonnull final String text,
                                                    @Nonnull final IExpressionFragment expected) {
        assertThat(lastExpression(text)).isEqualTo(expected);
    }

    @Test
    void parseProgramClosureOnNextLineIsStatementOfItsOwn() {
        final List<IStatementFragment> statements =
                parseProgram("extern func run(f: () -> int) -> int\nlet e = run\n{ 42 }").statements();
        // a trailing closure stays on the line of what it calls, so `let e = run` ends with its line
        assertThat(statements).hasSize(3);
        assertThat(statements.get(1)).isEqualTo(new IAssignmentFragment("e", reference(declaration("run",
                ImmutableList.of(), ImmutableList.of(parameter("f", function(ImmutableList.of(), INT))), INT))));
        assertThat(statements.get(2)).isEqualTo(closure(ImmutableList.of(), INT, new ILiteralFragment(42, INT)));
    }

    @Nonnull
    static Stream<Arguments> parenthesizedExpressions() {
        final IFunctionDeclarationFragment f =
                declaration("f", ImmutableList.of(), ImmutableList.of(parameter("x", INT)), INT);
        final IFunctionType intToBigint = function(ImmutableList.of(INT), BIGINT);
        final IFunctionCallFragment callK = new IFunctionCallFragment(reference(declaration("k", ImmutableList.of(),
                ImmutableList.of(), new IRecordType(false, ImmutableList.of(IRecordType.Field.of(intToBigint, "g"))))),
                ImmutableList.of());
        final IFunctionType bigintToBigint = function(ImmutableList.of(BIGINT), BIGINT);
        return Stream.of(
                // parentheses only group: the value and the type are those of the expression in them
                Arguments.of("let e = (42)", new ILiteralFragment(42, INT)),
                Arguments.of("let e = ((42))", new ILiteralFragment(42, INT)),
                // the context of the parentheses is the context of the expression in them
                Arguments.of("let e: bigint = (42)", new ILiteralFragment(42L, BIGINT)),
                Arguments.of(ID + "let e: bigint = (id)(x: 42)",
                        new IFunctionCallFragment(reference(ID_FUNCTION, ImmutableList.of(BIGINT), bigintToBigint),
                                ImmutableList.of(new ILiteralFragment(42L, BIGINT)))),
                // a line break may follow the ( and precede the )
                Arguments.of("let e = (\n    42\n)", new ILiteralFragment(42, INT)),
                // a postfix applies to the value in the parentheses, so a field of a function type can be called
                Arguments.of("extern func k() -> record<g: (int) -> bigint>\nlet e = (k().g)(1)",
                        new IFunctionCallFragment(new IFieldAccessFragment(callK, 0),
                                ImmutableList.of(new ILiteralFragment(1, INT)))),
                // parentheses are transparent for labels
                Arguments.of("extern func f(x: int) -> int\nlet e = (f)(x: 1)",
                        new IFunctionCallFragment(reference(f), ImmutableList.of(new ILiteralFragment(1, INT)))),
                // a closure in parentheses can be called, as without them
                Arguments.of("let e = ({ (y: int) -> int in y })(2)",
                        new IFunctionCallFragment(closure(ImmutableList.of(new IClosureFragment.Parameter("y", INT)),
                                INT, new IVariableReferenceFragment("y", INT)),
                                ImmutableList.of(new ILiteralFragment(2, INT)))));
    }

    @ParameterizedTest
    @MethodSource("parenthesizedExpressions")
    void parseProgramParenthesizedExpressionIsExpressionInParentheses(@Nonnull final String text,
                                                                      @Nonnull final IExpressionFragment expected) {
        assertThat(lastExpression(text)).isEqualTo(expected);
    }

    @Test
    void parseProgramParenthesisOnNextLineStartsStatement() {
        final List<IStatementFragment> statements =
                parseProgram("extern func f(x: int) -> int\nlet e = f\n(1)").statements();
        // the ( of a call stays on the line of what it calls, so `let e = f` ends with its line, and (1) is a
        // statement of its own, as in Swift
        assertThat(statements).hasSize(3);
        assertThat(statements.get(1)).isEqualTo(new IAssignmentFragment("e",
                reference(declaration("f", ImmutableList.of(), ImmutableList.of(parameter("x", INT)), INT))));
        assertThat(statements.get(2)).isEqualTo(new ILiteralFragment(1, INT));
    }

    @Test
    void parseProgramParenthesizedConditionTakesTrailingClosure() {
        final List<IStatementFragment> statements = parseProgram("""
                extern func contains(_ xs: [int], _ p: (int) -> boolean) -> boolean
                extern func numbers() -> [int]
                func f() {
                    if (numbers().contains { x in true }) { }
                }""").statements();
        final IFunctionDeclarationFragment contains = declaration("contains", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter(null, "xs", new IArrayType(false, INT)),
                        new IFunctionDefinitionFragment.Parameter(null, "p", function(ImmutableList.of(INT), BOOLEAN))),
                BOOLEAN);
        final IFunctionCallFragment numbers = new IFunctionCallFragment(reference(declaration("numbers",
                ImmutableList.of(), ImmutableList.of(), new IArrayType(false, INT))), ImmutableList.of());
        // in the parentheses, the condition is a full expression, so its call can take a trailing closure
        assertThat(((IFunctionDefinitionFragment)statements.get(2)).body().statements()).containsExactly(
                new IIfFragment(new IFunctionCallFragment(reference(contains), ImmutableList.of(numbers,
                        closure(ImmutableList.of(new IClosureFragment.Parameter("x", INT)), BOOLEAN,
                                new ILiteralFragment(true, BOOLEAN)))), EMPTY_BLOCK, null));
    }

    @Nonnull
    static Stream<Arguments> recordConstructors() {
        final ILiteralFragment one = new ILiteralFragment(1, INT);
        final ILiteralFragment two = new ILiteralFragment(2, INT);
        final ILiteralFragment a = new ILiteralFragment("a", STRING);
        final IRecordType intAndInt = new IRecordType(false,
                ImmutableList.of(IRecordType.Field.unnamedOf(INT), IRecordType.Field.unnamedOf(INT)));
        final IRecordConstructorFragment oneAndTwo = record(false, ImmutableList.of(unnamed(one), unnamed(two)));
        final IRecordConstructorFragment idAndName =
                record(false, ImmutableList.of(named(one, "id"), named(a, "name")));
        final IGenericType k = new IGenericType("K", false);
        final IType range = new IExternType(new ITypeDeclarationFragment("range", ImmutableList.of(), null), false);
        final IType nullableString = STRING.withNullability(true);
        final IRecordType key = new IRecordType(false,
                ImmutableList.of(IRecordType.Field.unnamedOf(INT), IRecordType.Field.unnamedOf(nullableString)));
        final IFunctionDeclarationFragment rangeFunction = declaration("range", ImmutableList.of(k),
                ImmutableList.of(parameter("closed", k), parameter("open", k)), range);
        final IFunctionDeclarationFragment g = declaration("g", ImmutableList.of(T),
                ImmutableList.of(parameter("x", T.withNullability(true))), T);
        final IFunctionDeclarationFragment first = declaration("first", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter(null, "r", intAndInt)), INT);
        final IRecordType noFields = new IRecordType(false, ImmutableList.of());
        final IRecordConstructorFragment empty = record(false, ImmutableList.of());
        final IFunctionDeclarationFragment recordFunction = declaration("record", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter(null, "x", INT)), INT);
        final IFunctionDeclarationFragment ofFunction =
                declaration("of", ImmutableList.of(), ImmutableList.of(parameter("x", INT)), INT);
        return Stream.of(
                // the fields have the values in order; a label is the name of its field, and a field can have none
                Arguments.of("let e = (1, 2)", oneAndTwo),
                Arguments.of("let e = (id: 1, name: \"a\")", idAndName),
                Arguments.of("let e = (id: 1, \"a\")", record(false, ImmutableList.of(named(one, "id"), unnamed(a)))),
                // in the short form, a record of one field has a label, and a label can be any name between backticks
                Arguments.of("let e = (id: 1)", record(false, ImmutableList.of(named(one, "id")))),
                Arguments.of("let e = (`first name`: \"a\", `_`: 1)",
                        record(false, ImmutableList.of(named(a, "first name"), named(one, "_")))),
                // a line break may follow the (, precede the ), and come before or after a , or a :
                Arguments.of("let e = (\n    id: 1,\n    name: \"a\"\n)", idAndName),
                Arguments.of("let e = (id\n: 1\n, name:\n\"a\")", idAndName),
                // the context gives the values their types
                Arguments.of("let e: record<bigint, string?> = (10, nil)", record(false, ImmutableList.of(
                        unnamed(new ILiteralFragment(10L, BIGINT)),
                        unnamed(new ILiteralFragment(null, nullableString))))),
                Arguments.of("let e: record<int?, int> = (1, 2)", record(false,
                        ImmutableList.of(unnamed(new ILiteralFragment(1, INT.withNullability(true))), unnamed(two)))),
                Arguments.of("let e: record<f: (int) -> int, n: int> = (f: { x in x }, n: 1)", record(false,
                        ImmutableList.of(named(closure(ImmutableList.of(new IClosureFragment.Parameter("x", INT)), INT,
                                new IVariableReferenceFragment("x", INT)), "f"), named(one, "n")))),
                // the context can make the record nullable, as it can make a literal nullable
                Arguments.of("let e: record<int, int>? = (1, 2)",
                        record(true, ImmutableList.of(unnamed(one), unnamed(two)))),
                Arguments.of("extern func g<T>(x: T?) -> T\nlet e = g(x: (1, 2))",
                        new IFunctionCallFragment(reference(g, ImmutableList.of(intAndInt),
                                function(ImmutableList.of(intAndInt.withNullability(true)), intAndInt)),
                                ImmutableList.of(record(true, ImmutableList.of(unnamed(one), unnamed(two)))))),
                Arguments.of(PAIR + "pair(a: (1, 2), b: nil)", new IFunctionCallFragment(reference(PAIR_FUNCTION,
                        ImmutableList.of(intAndInt.withNullability(true)), function(ImmutableList.of(
                                intAndInt.withNullability(true), intAndInt.withNullability(true)), VOID)),
                        ImmutableList.of(record(true, ImmutableList.of(unnamed(one), unnamed(two))),
                                new ILiteralFragment(null, intAndInt.withNullability(true))))),
                // without such a context, the record is not nullable
                Arguments.of(ID + "let e = id(x: (1, 2))", new IFunctionCallFragment(reference(ID_FUNCTION,
                        ImmutableList.of(intAndInt), function(ImmutableList.of(intAndInt), intAndInt)),
                        ImmutableList.of(oneAndTwo))),
                // two records of one type: the values of each give the types of the other one's
                Arguments.of(PAIR + "pair(a: (1, nil), b: (2, \"a\"))", new IFunctionCallFragment(reference(
                        PAIR_FUNCTION, ImmutableList.of(key), function(ImmutableList.of(key, key), VOID)),
                        ImmutableList.of(record(false, ImmutableList.of(unnamed(one),
                                        unnamed(new ILiteralFragment(null, nullableString)))),
                                record(false, ImmutableList.of(unnamed(two),
                                        unnamed(new ILiteralFragment("a", nullableString))))))),
                // the range of the sketch: the nil of the open end makes the second field of the key nullable
                Arguments.of("extern type range\nextern func range<K>(closed: K, open: K) -> range\n"
                                + "let e = range(closed: (5, \"Hello\"), open: (10, nil))",
                        new IFunctionCallFragment(reference(rangeFunction, ImmutableList.of(key),
                                function(ImmutableList.of(key, key), range)), ImmutableList.of(
                                record(false, ImmutableList.of(unnamed(new ILiteralFragment(5, INT)),
                                        unnamed(new ILiteralFragment("Hello", nullableString)))),
                                record(false, ImmutableList.of(unnamed(new ILiteralFragment(10, INT)),
                                        unnamed(new ILiteralFragment(null, nullableString))))))),
                // records nest
                Arguments.of("let e = ((1, 2), c: (d: 1))", record(false,
                        ImmutableList.of(unnamed(oneAndTwo), named(record(false, ImmutableList.of(named(one, "d"))),
                                "c")))),
                // a postfix applies to the record: a field access, also by ordinal, and a dot call
                Arguments.of("let e = (id: 1, name: \"a\").name", new IFieldAccessFragment(idAndName, 1)),
                Arguments.of("let e = (1, 2).1", new IFieldAccessFragment(oneAndTwo, 1)),
                Arguments.of("extern func first(_ r: record<int, int>) -> int\nlet e = (1, 2).first()",
                        new IFunctionCallFragment(reference(first), ImmutableList.of(oneAndTwo))),
                // a record can be the value of a closure, and a closure whose body starts with a ( has no signature
                // unless an in follows
                Arguments.of("let e: () -> record<int, int> = { (1, 2) }",
                        closure(ImmutableList.of(), intAndInt, oneAndTwo)),
                Arguments.of("let e = { () -> record<id: int> in (id: 1) }", closure(ImmutableList.of(),
                        new IRecordType(false, ImmutableList.of(IRecordType.Field.of(INT, "id"))),
                        record(false, ImmutableList.of(named(one, "id"))))),
                // a stream block can yield records
                Arguments.of("let e = stream { yield (1, 2) }", stream(intAndInt, ImmutableList.of(oneAndTwo))),
                // the long form constructs the same records, and also a record of one field without a name and a
                // record without fields
                Arguments.of("let e = record.of(1, 2)", oneAndTwo),
                Arguments.of("let e = record.of(id: 1, name: \"a\")", idAndName),
                Arguments.of("let e = record.of(id: 1)", record(false, ImmutableList.of(named(one, "id")))),
                Arguments.of("let e = record.of(1)", record(false, ImmutableList.of(unnamed(one)))),
                Arguments.of("let e = record.of()", empty),
                // the long form has the line breaks of the short form, and its . may start a line
                Arguments.of("let e = record.of(\n    1,\n    2\n)", oneAndTwo),
                Arguments.of("let e = record\n    .of(1)", record(false, ImmutableList.of(unnamed(one)))),
                // the context gives the values their types and can make the record nullable, also in the long form
                Arguments.of("let e: record<bigint> = record.of(1)",
                        record(false, ImmutableList.of(unnamed(new ILiteralFragment(1L, BIGINT))))),
                Arguments.of("let e: record<>? = record.of()", record(true, ImmutableList.of())),
                Arguments.of(ID + "let e = id(x: record.of())", new IFunctionCallFragment(reference(ID_FUNCTION,
                        ImmutableList.of(noFields), function(ImmutableList.of(noFields), noFields)),
                        ImmutableList.of(empty))),
                // the two forms nest in each other, and a postfix applies to a record of one field
                Arguments.of("let e = record.of(record.of(), (1, 2))",
                        record(false, ImmutableList.of(unnamed(empty), unnamed(oneAndTwo)))),
                Arguments.of("let e = record.of(1).0",
                        new IFieldAccessFragment(record(false, ImmutableList.of(unnamed(one))), 0)),
                // type arguments state the fields, also their numbers, and a value has the name of its field as its
                // label
                Arguments.of("let e = record.of<int>(1)", record(false, ImmutableList.of(unnamed(one)))),
                Arguments.of("let e = record.of<id: int, name: string>(id: 1, name: \"a\")", idAndName),
                Arguments.of("let e = record.of<id(1): bigint, string?>(id: 5, nil)", record(false, ImmutableList.of(
                        IRecordConstructorFragment.Field.of(new ILiteralFragment(5L, BIGINT), "id", 1),
                        unnamed(new ILiteralFragment(null, nullableString))))),
                Arguments.of("let e = record.of<>()", empty),
                Arguments.of("typealias key = bigint\nlet e = record.of<id: key>(id: 5)",
                        record(false, ImmutableList.of(named(new ILiteralFragment(5L, BIGINT), "id")))),
                // the type arguments state no nullability, so the context can still make the record nullable
                Arguments.of("let e: record<int>? = record.of<int>(1)",
                        record(true, ImmutableList.of(unnamed(one)))),
                // a field access reads a field that the type arguments state
                Arguments.of("let e = record.of<id(1): bigint>(id: 5).id", new IFieldAccessFragment(record(false,
                        ImmutableList.of(IRecordConstructorFragment.Field.of(new ILiteralFragment(5L, BIGINT), "id",
                                1))), 0)),
                // of is not a keyword, so it can name a function, and record between backticks is a name too
                Arguments.of("extern func of(x: int) -> int\nlet e = of(x: 1)",
                        new IFunctionCallFragment(reference(ofFunction), ImmutableList.of(one))),
                Arguments.of("extern func `record`(_ x: int) -> int\nlet e = `record`(1)",
                        new IFunctionCallFragment(reference(recordFunction), ImmutableList.of(one))));
    }

    @ParameterizedTest
    @MethodSource("recordConstructors")
    void parseProgramRecordConstructorBuildsRecordOfFields(@Nonnull final String text,
                                                          @Nonnull final IExpressionFragment expected) {
        assertThat(lastExpression(text)).isEqualTo(expected);
    }

    @Test
    void parseProgramRecordConstructorOnNextLineStartsStatement() {
        final List<IStatementFragment> statements =
                parseProgram("extern func f(x: int) -> int\nlet e = f\n(1, 2)").statements();
        // as for a parenthesized expression, the ( on the next line starts a statement of its own
        assertThat(statements).hasSize(3);
        assertThat(statements.get(2)).isEqualTo(record(false,
                ImmutableList.of(unnamed(new ILiteralFragment(1, INT)), unnamed(new ILiteralFragment(2, INT)))));
    }

    @Test
    void parseProgramRecordConstructorIsValueOfFunctionBody() {
        // the projection of the sketch, which the result type of the function gives its type
        final List<IStatementFragment> statements = parseProgram("""
                func f(r: record<name: string?, rating: int>) -> record<string?, int> {
                    (r.name, r.rating)
                }""").statements();
        final IVariableReferenceFragment r = new IVariableReferenceFragment("r", new IRecordType(false,
                ImmutableList.of(IRecordType.Field.of(STRING.withNullability(true), "name"),
                        IRecordType.Field.of(INT, "rating"))));
        assertThat(((IFunctionDefinitionFragment)statements.get(0)).body().statements()).containsExactly(
                record(false, ImmutableList.of(unnamed(new IFieldAccessFragment(r, 0)),
                        unnamed(new IFieldAccessFragment(r, 1)))));
    }

    @Test
    void parseProgramRecordConstructorTypeArgumentsNameTypeParameter() {
        // the type arguments of record.of are types in the scope of the constructor, so they can name a type parameter
        final List<IStatementFragment> statements =
                parseProgram("func f<T>(x: T) -> record<T> { record.of<T>(x) }").statements();
        assertThat(((IFunctionDefinitionFragment)statements.get(0)).body().statements()).containsExactly(
                record(false, ImmutableList.of(unnamed(new IVariableReferenceFragment("x", T)))));
    }

    @Nonnull
    static Stream<Arguments> metaConstructors() {
        final IRecordType idRecord = new IRecordType(false, ImmutableList.of(IRecordType.Field.of(INT, "id")));
        final IMetaType metaOfT = new IMetaType(T);
        final IFunctionDeclarationFragment scan = declaration("scan", ImmutableList.of(T),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter(null, "recordType", STRING),
                        parameter("as", metaOfT)), new IStreamType(T));
        return Stream.of(
                // a token stands for its type argument, and its type is the meta type of that type
                Arguments.of("let e = meta.of<int>()", new IMetaConstructorFragment(INT)),
                Arguments.of("let e = meta.of<record<id: int>>()", new IMetaConstructorFragment(idRecord)),
                Arguments.of("typealias row = record<id: int>\nlet e = meta.of<row>()",
                        new IMetaConstructorFragment(idRecord)),
                // as for a generic function, the context gives the type if there is no type argument
                Arguments.of("let e: meta<bigint> = meta.of()", new IMetaConstructorFragment(BIGINT)),
                // the . may start a line
                Arguments.of("let e = meta\n    .of<int>()", new IMetaConstructorFragment(INT)),
                // a token fixes the type parameter of the function that it is passed to, and so its result type
                Arguments.of(SCAN + "let e = scan(\"restaurants\", as: meta.of<record<id: int>>())",
                        new IFunctionCallFragment(reference(scan, ImmutableList.of(idRecord),
                                function(ImmutableList.of(STRING, new IMetaType(idRecord)), new IStreamType(idRecord))),
                                ImmutableList.of(new ILiteralFragment("restaurants", STRING),
                                        new IMetaConstructorFragment(idRecord)))),
                // without a type argument, the result type of the call fixes the type of the token
                Arguments.of(SCAN + "let e: stream<bigint> = scan(\"numbers\", as: meta.of())",
                        new IFunctionCallFragment(reference(scan, ImmutableList.of(BIGINT),
                                function(ImmutableList.of(STRING, new IMetaType(BIGINT)), new IStreamType(BIGINT))),
                                ImmutableList.of(new ILiteralFragment("numbers", STRING),
                                        new IMetaConstructorFragment(BIGINT)))));
    }

    @ParameterizedTest
    @MethodSource("metaConstructors")
    void parseProgramMetaConstructorMakesTokenOfItsType(@Nonnull final String text,
                                                       @Nonnull final IExpressionFragment expected) {
        assertThat(lastExpression(text)).isEqualTo(expected);
    }

    @Test
    void parseProgramMetaConstructorTypeArgumentNamesTypeParameter() {
        // the runtime specializes a function before it runs it, so a token can stand for a type parameter, and a
        // generic function can also pass a token that it receives on
        final List<IStatementFragment> statements = parseProgram(SCAN + """
                func token<T>(_ x: T) -> meta<T> { meta.of<T>() }
                func scanAll<T>(_ name: string, as: meta<T>) -> stream<T> { scan(name, as: as) }""").statements();
        assertThat(((IFunctionDefinitionFragment)statements.get(1)).body().statements())
                .containsExactly(new IMetaConstructorFragment(T));
        final IFunctionCallFragment call =
                (IFunctionCallFragment)((IFunctionDefinitionFragment)statements.get(2)).body().statements().get(0);
        assertThat(call.arguments().get(1)).isEqualTo(new IVariableReferenceFragment("as", new IMetaType(T)));
        assertThat(call.type()).isEqualTo(new IStreamType(T));
    }

    @Test
    void parseProgramMetaConstructorSelectsVariantByType() {
        // the type of a token can select the variant of an overloaded function, and so the type of its result
        final String parse = """
                overloaded func parse<T>(_ s: string, as: meta<T>)
                extern variant func parse<int>(_ s: string, as: meta<int>) -> int
                extern variant func parse<bigint>(_ s: string, as: meta<bigint>) -> bigint
                """;
        assertThat(lastExpression(parse + "let e = parse(\"42\", as: meta.of<int>())").type()).isEqualTo(INT);
        assertThat(lastExpression(parse + "let e = parse(\"42\", as: meta.of<bigint>())").type()).isEqualTo(BIGINT);
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
            ID + "let e = id(x: nil)",
            // there is no promotion: an int is not an int?, and a statement is solved on its own, so `a` is an int
            "extern func f(x: int?)\nfunc g(y: int) { f(x: y) }",
            "extern func f(x: bigint)\nlet a = 42\nf(x: a)",
            // also in a function body, unlike in a closure
            "extern func f(x: bigint)\nfunc g() {\n    let a = 42\n    f(x: a)\n}",
            // a call must match the parameters of the function
            "extern func f(x: int)\nf()",
            "extern func f(x: int)\nf(1, 2)",
            "extern func f(x: int)\nf(x: \"a\")",
            PAIR + "pair(a: 42, b: \"a\")",
            // only a function that is not nullable can be called
            "let e = 42(1)",
            "func f(h: ((int) -> int)?) { h(1) }",
            // a type argument cannot be void where it becomes the type of a parameter
            ID + "extern func g()\nid(x: g())",
            // without a context, the types of the parameters of a closure cannot be inferred
            "let e = { x in }",
            "let e = { x in x }",
            "let e = { x, y in }",
            "let e = { (x, y: int) in }",
            // the closure must fit its context: the result type, the number of parameters, and void
            "extern func foo(s: string, f: (int) -> string)\nfoo(s: \"hello\", f: { i in 42 })",
            "extern func h(f: (int) -> int)\nh(f: { x, y in x })",
            "extern func g(x: int) -> int\nextern func h(f: (int) -> void)\nh(f: { i in g(x: i) })",
            // a closure whose result type is not void must have a value of that type
            "let e = { () -> int in }",
            // a stream block yields values of its element type, which must be inferred and cannot be void
            "let e: stream<int> = stream { yield \"a\" }",
            "let e = stream { yield 1; yield \"a\" }",
            "let e = stream { }",
            "extern func g()\nlet e = stream { yield g() }",
            // a stream block is a stream, which is neither of another type nor a function
            "let e: int = stream { yield 1 }",
            "extern func h(f: () -> stream<int>)\nh(f: stream { yield 1 })",
            "let e = stream { yield 1 }()",
            // the type of a parameter cannot contain itself
            "let e = { f in f(f) }",
            // a function nested in a closure cannot use a variable of the closure whose type is not known yet
            "let e = { x in func g<T>(y: T) -> T { x } }",
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
            // without arguments or a trailing closure, a dot is a field access, not a call, and 1 is not a record
            "extern func inc(x: int) -> int\nlet e = 1.inc",
            // a dot call calls a function in scope, not a variable of function type
            "let e = 1.inc()",
            "let inc = { (x: int) -> int in x }\nlet e = 1.inc()",
            // a statement cannot start with a dot, and the name stays on the line of the dot
            "extern func inc(x: int) -> int\nlet e = 1;.inc()",
            "extern func inc(x: int) -> int\nlet e = 1.\ninc()",
            // a dot call needs the arguments of all the other parameters
            "extern func f(x: int, y: int) -> int\nlet e = 1.f()",
            // a trailing closure is an argument like any other: it must be of the type of the last parameter
            "extern func inc(x: int) -> int\nlet e = inc { 1 }",
            // a call has one trailing closure at most; a second one calls the result of the call
            "extern func fold(x: int, f: (int) -> int) -> int\nlet e = fold(x: 1) { y in y } { z in z }",
            // a field access needs a field of its record, by name or by ordinal
            "extern func r() -> record<id: bigint, name: string?>\nlet e = r().nope",
            "extern func r() -> record<id: bigint, name: string?>\nlet e = r().2",
            "extern func r() -> record<id: bigint, name: string?>\nlet e = r().99999999999",
            "extern func r() -> record<id: bigint, name: string?>\nlet e = r().1e0",
            // a field access needs a record that is not nullable
            "extern func r() -> record<a: int>?\nlet e = r().a",
            "extern func r() -> record<a: int>?\nlet e = r().0",
            "let x = 1\nlet e = x.a",
            "let x = 1\nlet e = x.0",
            // the type of nil, and a type T? whose T is still open, are nullable
            "let e = nil.a",
            "extern func f<T, U>(g: (T?) -> U, x: T) -> U\nlet e = f(g: { v in v.a }, x: 1)",
            // the type of the record must be known at the access; in apply, the closure comes before the record
            "let e = { v in v.a }",
            APPLY + "extern func r() -> record<id: bigint>\nlet e = apply(g: { v in v.id }, x: r())",
            // with arguments, a dot is a dot call of a function, also if the record has a field of the name
            "extern func k() -> record<g: () -> int>\nlet e = k().g()",
            // the type of the field is the type of the access
            "extern func r() -> record<id: bigint, name: string?>\nlet e: int = r().name",
            // parentheses hold one expression without a label, and () is not an expression
            "let e = ()",
            "let e = (\n)",
            "let e = (1",
            "let e = 1)",
            // parentheses are transparent for labels
            "extern func f(x: int) -> int\nlet e = (f)(1)",
            // in the short form, a record of one field needs a label, and no comma follows the last field
            "let e = (1,)",
            "let e = (x: 1,)",
            "let e = (1, 2,)",
            // the label _ needs backticks, and the names of a record are distinct
            "let e = (_: 1, 2)",
            "let e = (a: 1, a: 2)",
            // the field names and the field numbers are those of the type that the context requires
            "let e: record<id: int, name: string> = (1, \"a\")",
            "let e: record<int, string> = (id: 1, name: \"a\")",
            "let e: record<id(1): int> = (id: 1)",
            "let e: record<a: int>? = (b: 1)",
            PAIR + "pair(a: (1, 2), b: (x: 3, 4))",
            // the values must have the types of the fields, and there must be as many values as fields
            "let e: record<int, string> = (1, 2)",
            "let e: record<a: int> = (1, 2)",
            "let e = (1, nil)",
            "extern func g()\nlet e = (1, g())",
            // a record is neither of a primitive type nor a function
            "let e: int = (1, 2)",
            "let e: int? = (1, 2)",
            "let e = (1, 2)(3)",
            // a field access needs a field of the record
            "let e = (a: 1).b",
            "let e = (a: 1, 2).2",
            // a field access needs a record that is not nullable, so the record cannot also be where nil is
            PAIR + "let e = { () -> int in\n    let r = (x: 1)\n    pair(a: r, b: nil)\n    r.x\n}",
            PAIR + "let e = { () -> int in\n    let r = (x: 1)\n    let y = r.x\n    pair(a: r, b: nil)\n    y\n}",
            // a closure whose returns have values returns a value on every path, and its returns all have one
            "let e = { (c: boolean) in\n    if c { return 1 }\n    return\n}",
            "let e = { (c: boolean) in if c { return 1 } }",
            "let e = { () -> void in return 1 }",
            // as in Swift, a body of more than one statement gives its value with a return
            "let e = { () -> int in\n    let a = 1\n    a\n}",
            "extern func h(f: (int) -> int)\nh(f: { x in\n    let y = x\n    y\n})",
            // no statement can follow a return
            "let e = { (x: int) in\n    return x\n    x\n}",
            // the type of a record cannot contain itself
            PAIR + "let e = { x in pair(a: x, b: (a: x)) }",
            PAIR + "let e = { x in pair(a: (a: x), b: x) }",
            // the long form is record.of; its ( stays on the line of record.of, and no comma follows its last field
            "let e = record(1)",
            "let e = record",
            "let e = record.of",
            "let e = record.make(1)",
            "let e = record.of\n(1)",
            "let e = record.\nof(1)",
            "let e = record.of(1,)",
            // the long form has the rules of the short form for labels, names, types, and field accesses
            "let e = record.of(_: 1)",
            "let e = record.of(a: 1, a: 2)",
            "let e: record<a: int> = record.of(1)",
            "let e: record<> = record.of(1)",
            "let e: record<int> = record.of()",
            "let e = record.of().a",
            "let e = record.of()()",
            // type arguments state one field for each value, and the labels follow the rule of a call
            "let e = record.of<int>()",
            "let e = record.of<int>(1, 2)",
            "let e = record.of<id: int>(1)",
            "let e = record.of<id: int>(name: 1)",
            "let e = record.of<int>(x: 1)",
            "let e = record.of<int>\n(1)",
            // the values have the types that the type arguments state, which must be the type of the context
            "let e = record.of<int>(\"a\")",
            "let e: record<bigint> = record.of<int>(1)",
            "let e: record<id: int> = record.of<id(1): int>(id: 1)",
            // the type arguments are a record type: their names are distinct, and no field is void
            "let e = record.of<a: int, a: int>(a: 1, a: 2)",
            "extern func g()\nlet e = record.of<void>(g())",
            // meta.of makes a token of one type, which is not void and which must be known, and it takes no arguments
            "let e = meta.of()",
            "let e = meta.of<int, int>()",
            "let e = meta.of<void>()",
            "let e = meta.make<int>()",
            "let e = meta.of<int>(1)",
            "let e = meta.of<int>\n()",
            "let e: meta<int> = meta.of<bigint>()",
            // a type is not a value, so only a token stands for a type
            "let e = meta",
            "let e = meta<int>",
            SCAN + "let e = scan(\"restaurants\", as: record<id: int>)",
            SCAN + "let e = scan(\"restaurants\", as: 1)",
            // a program can do nothing with a token but pass it on
            "let e = meta.of<record<a: int>>().a",
            "let e = meta.of<int>()()",
    })
    void parseProgramInvalidExpressionFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Returns the expression of the last statement of a program, which is an assignment or an expression.
     */
    @Nonnull
    private static IExpressionFragment lastExpression(@Nonnull final String text) {
        final List<IStatementFragment> statements = parseProgram(text).statements();
        final IStatementFragment last = statements.get(statements.size() - 1);
        return last instanceof IAssignmentFragment assignment ? assignment.expression() : (IExpressionFragment)last;
    }

    @Nonnull
    private static IFunctionType function(@Nonnull final List<IType> parameterTypes, @Nonnull final IType resultType) {
        return new IFunctionType(false, parameterTypes, resultType);
    }

    /**
     * Returns a reference to a function that is not generic, whose type is the type of the function.
     */
    @Nonnull
    private static IFunctionReferenceFragment reference(@Nonnull final IFunctionFragment function) {
        return reference(function, ImmutableList.of(), function.type());
    }

    @Nonnull
    private static IFunctionReferenceFragment reference(@Nonnull final IFunctionFragment function,
                                                        @Nonnull final List<IType> typeArguments,
                                                        @Nonnull final IFunctionType type) {
        return new IFunctionReferenceFragment(function, typeArguments, type);
    }

    /**
     * Returns the declaration of an extern function, as a program without a package and without annotations declares it.
     */
    @Nonnull
    private static IFunctionDeclarationFragment declaration(
            @Nonnull final String name, @Nonnull final List<IGenericType> typeParameters,
            @Nonnull final List<IFunctionDefinitionFragment.Parameter> parameters, @Nonnull final IType resultType) {
        return new IFunctionDeclarationFragment(name, typeParameters, parameters, resultType);
    }

    @Nonnull
    private static IFunctionDefinitionFragment.Parameter parameter(@Nonnull final String name,
                                                                   @Nonnull final IType type) {
        return new IFunctionDefinitionFragment.Parameter(name, type);
    }

    /**
     * Returns a closure whose body is a single expression, which is the value of the body.
     */
    @Nonnull
    private static IClosureFragment closure(@Nonnull final List<IClosureFragment.Parameter> parameters,
                                            @Nonnull final IType resultType,
                                            @Nonnull final IExpressionFragment value) {
        return new IClosureFragment(parameters, resultType, new IBlockFragment(ImmutableList.of(value)));
    }

    /**
     * Returns a stream block whose block yields the given values in order.
     */
    @Nonnull
    private static IStreamBlockFragment stream(@Nonnull final IType elementType,
                                               @Nonnull final List<IExpressionFragment> values) {
        return new IStreamBlockFragment(elementType, new IBlockFragment(values.stream()
                .<IStatementFragment>map(IYieldFragment::new)
                .collect(ImmutableList.toImmutableList())));
    }

    @Nonnull
    private static IRecordConstructorFragment record(final boolean isNullable,
                                                     @Nonnull final List<IRecordConstructorFragment.Field> fields) {
        return new IRecordConstructorFragment(isNullable, fields);
    }

    @Nonnull
    private static IRecordConstructorFragment.Field named(@Nonnull final IExpressionFragment value,
                                                          @Nonnull final String name) {
        return IRecordConstructorFragment.Field.of(value, name);
    }

    @Nonnull
    private static IRecordConstructorFragment.Field unnamed(@Nonnull final IExpressionFragment value) {
        return IRecordConstructorFragment.Field.unnamedOf(value);
    }
}
