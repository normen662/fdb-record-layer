/*
 * FrlIrProgramParsingTest.java
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

import com.apple.foundationdb.record.ir.fragments.IAnnotationFragment;
import com.apple.foundationdb.record.ir.fragments.IArrayType;
import com.apple.foundationdb.record.ir.fragments.IAssignmentFragment;
import com.apple.foundationdb.record.ir.fragments.IBlockFragment;
import com.apple.foundationdb.record.ir.fragments.IClosureFragment;
import com.apple.foundationdb.record.ir.fragments.IConsumeFragment;
import com.apple.foundationdb.record.ir.fragments.IExternType;
import com.apple.foundationdb.record.ir.fragments.IForFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionCallFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionDeclarationFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionDefinitionFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionReferenceFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.apple.foundationdb.record.ir.fragments.IIfFragment;
import com.apple.foundationdb.record.ir.fragments.ILiteralFragment;
import com.apple.foundationdb.record.ir.fragments.IPackageStatementFragment;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IProgramFragment;
import com.apple.foundationdb.record.ir.fragments.IRecordType;
import com.apple.foundationdb.record.ir.fragments.IStatementFragment;
import com.apple.foundationdb.record.ir.fragments.IStreamBlockFragment;
import com.apple.foundationdb.record.ir.fragments.IStreamType;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.fragments.ITypeAliasFragment;
import com.apple.foundationdb.record.ir.fragments.ITypeDeclarationFragment;
import com.apple.foundationdb.record.ir.fragments.IVariableReferenceFragment;
import com.apple.foundationdb.record.ir.fragments.IYieldBreakFragment;
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
 * Tests for parsing FRL-IR programs: the program, statement, function definition, function declaration, type
 * declaration, type alias, package, annotation, yield, and control-flow productions of the grammar, and the fragments
 * that {@link FrlIrFragmentVisitor} builds from them; the stream blocks that yields belong to are tested with the other
 * expressions.
 */
class FrlIrProgramParsingTest {
    private static final IType INT = new IPrimitiveType(IPrimitiveType.Kind.INT, false);
    private static final IType BIGINT = new IPrimitiveType(IPrimitiveType.Kind.BIGINT, false);
    private static final IType STRING = new IPrimitiveType(IPrimitiveType.Kind.STRING, false);
    private static final IType BOOLEAN = new IPrimitiveType(IPrimitiveType.Kind.BOOLEAN, false);
    private static final IType VOID = new IPrimitiveType(IPrimitiveType.Kind.VOID, false);
    private static final IType STOP_REASON = IConsumeFragment.STOP_REASON_TYPE;
    private static final IGenericType T = new IGenericType("T", false);
    private static final IGenericType U = new IGenericType("U", false);
    private static final IBlockFragment EMPTY_BLOCK = new IBlockFragment(ImmutableList.of());

    @Nonnull
    static Stream<Arguments> validPrograms() {
        final IFunctionDefinitionFragment a = function("a");
        final IFunctionDefinitionFragment b = function("b");
        return Stream.of(
                // a program is a sequence of statements, separated as in a block
                Arguments.of("", new IProgramFragment(ImmutableList.of())),
                Arguments.of("\n\n;\n", new IProgramFragment(ImmutableList.of())),
                Arguments.of("func a() { }", new IProgramFragment(ImmutableList.of(a))),
                Arguments.of("func a() { }; func b() { }", new IProgramFragment(ImmutableList.of(a, b))),
                Arguments.of("\nfunc a() { }\n\nfunc b() { }\n", new IProgramFragment(ImmutableList.of(a, b))));
    }

    @ParameterizedTest
    @MethodSource("validPrograms")
    void parseProgramValidProgramCreatesProgram(@Nonnull final String text, @Nonnull final IProgramFragment expected) {
        assertThat(parseProgram(text)).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // statements on one line need a semicolon between them
            "func a() { } func b() { }",
            // a type parameter is in scope only within its function
            "func a<T>() { }\nfunc b(x: T) { }",
    })
    void parseProgramInvalidProgramFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Nonnull
    static Stream<Arguments> validFunctionDefinitions() {
        return Stream.of(
                // the value of the body is the result; the result type is the context of the last expression
                Arguments.of("func f(x: int, y: string?): bigint { 7 }", new IFunctionDefinitionFragment("f",
                        ImmutableList.of(), ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", INT),
                                new IFunctionDefinitionFragment.Parameter("y", STRING.withNullability(true))),
                        BIGINT, new IBlockFragment(ImmutableList.of(new ILiteralFragment(7L, BIGINT))))),
                // a trailing semicolon does not drop the value
                Arguments.of("func f(): int { 42; }", new IFunctionDefinitionFragment("f", ImmutableList.of(),
                        ImmutableList.of(), INT, new IBlockFragment(ImmutableList.of(new ILiteralFragment(42, INT))))),
                // without a result type, a function returns void, and a value of its body is discarded
                Arguments.of("func g() { }", new IFunctionDefinitionFragment("g", ImmutableList.of(),
                        ImmutableList.of(), VOID, EMPTY_BLOCK)),
                Arguments.of("func g(): () { }", new IFunctionDefinitionFragment("g", ImmutableList.of(),
                        ImmutableList.of(), VOID, EMPTY_BLOCK)),
                Arguments.of("func g() { 42 }", new IFunctionDefinitionFragment("g", ImmutableList.of(),
                        ImmutableList.of(), VOID, new IBlockFragment(ImmutableList.of(new ILiteralFragment(42, INT))))),
                // a function type after the colon keeps its arrow, and the body's closure takes its types from it
                Arguments.of("func adder(x: int): (int) -> int { { y in x } }", new IFunctionDefinitionFragment("adder",
                        ImmutableList.of(), ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", INT)),
                        new IFunctionType(false, ImmutableList.of(INT), INT),
                        new IBlockFragment(ImmutableList.of(new IClosureFragment(
                                ImmutableList.of(new IClosureFragment.Parameter("y", INT)), INT,
                                new IBlockFragment(ImmutableList.of(new IVariableReferenceFragment("x", INT)))))))),
                // generic functions: type parameters are types within the signature
                Arguments.of("func id<T>(x: T): T { x }", new IFunctionDefinitionFragment("id", ImmutableList.of(T),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", T)), T,
                        new IBlockFragment(ImmutableList.of(new IVariableReferenceFragment("x", T))))),
                Arguments.of("func map<T, U>(xs: [T], f: (T) -> U?, ys: [U?]): [U?] { ys }",
                        new IFunctionDefinitionFragment("map", ImmutableList.of(T, U),
                                ImmutableList.of(
                                        new IFunctionDefinitionFragment.Parameter("xs", new IArrayType(false, T)),
                                        new IFunctionDefinitionFragment.Parameter("f",
                                                new IFunctionType(false, ImmutableList.of(T), U.withNullability(true))),
                                        new IFunctionDefinitionFragment.Parameter("ys",
                                                new IArrayType(false, U.withNullability(true)))),
                                new IArrayType(false, U.withNullability(true)),
                                new IBlockFragment(ImmutableList.of(new IVariableReferenceFragment("ys",
                                        new IArrayType(false, U.withNullability(true))))))),
                // names between backticks
                Arguments.of("func `in`(`func`: int) { }", new IFunctionDefinitionFragment("in", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("func", INT)), VOID, EMPTY_BLOCK)));
    }

    @ParameterizedTest
    @MethodSource("validFunctionDefinitions")
    void parseProgramFunctionDefinitionCreatesDefinition(@Nonnull final String text,
                                                         @Nonnull final IFunctionDefinitionFragment expected) {
        assertThat(parseProgram(text).statements()).containsExactly(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "func (x: int) { }",              // no name
            "func f<>(x: int) { }",           // no type parameters between the angle brackets
            "func f(x) { }",                  // a parameter of a function definition states its type
            "func f(x: int): { }",            // a colon needs a result type
            "func f(x: int) -> int { }",      // the result type follows a colon, not an arrow
            "func f(x: int)",                 // no block
            "func f(x: T) { }",               // T is not a type parameter of f
            "func f<T>(x: U): T { }",         // nor is U
            "func f<T, T>(x: T) { }",         // type parameters have distinct names
            "f(x: int) { }",                  // no func keyword
            "func f(x: void) { }",            // void is only a result type
            // the body of a function whose result type is not void ends with a value of that type
            "func f(): int { }",
            "func f(): int { let x = 1 }",
            "func f(): int { \"a\" }",
            "func f(): int { func g() { } }",
            "func f(): int { extern func g(): int }",
            "func g(): int { 42 }\nfunc f(): int? { g() }",  // there is no promotion
    })
    void parseProgramInvalidFunctionDefinitionFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Nonnull
    static Stream<Arguments> validFunctionDeclarations() {
        return Stream.of(
                Arguments.of("extern func f(x: int): bigint", new IFunctionDeclarationFragment("f", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", INT)), BIGINT)),
                // without a result type, a declared function returns void
                Arguments.of("extern func g<T>(x: T)", new IFunctionDeclarationFragment("g", ImmutableList.of(T),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", T)), VOID)),
                // a declaration can be split across lines wherever it cannot end
                Arguments.of("extern\nfunc\nh(\n    x: int,\n    y: int\n): (int) -> int",
                        new IFunctionDeclarationFragment("h", ImmutableList.of(),
                                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", INT),
                                        new IFunctionDefinitionFragment.Parameter("y", INT)),
                                new IFunctionType(false, ImmutableList.of(INT), INT))));
    }

    @ParameterizedTest
    @MethodSource("validFunctionDeclarations")
    void parseProgramFunctionDeclarationCreatesDeclaration(@Nonnull final String text,
                                                           @Nonnull final IFunctionDeclarationFragment expected) {
        assertThat(parseProgram(text).statements()).containsExactly(expected);
    }

    @Test
    void parseProgramDeclaredFunctionCanBeCalled() {
        final IFunctionDeclarationFragment f = new IFunctionDeclarationFragment("f", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", BIGINT)), STRING);
        assertThat(parseProgram("extern func f(x: bigint): string\nlet s = f(x: 42)").statements()).containsExactly(f,
                new IAssignmentFragment("s", new IFunctionCallFragment(
                        new IFunctionReferenceFragment(f, ImmutableList.of(), f.type()),
                        ImmutableList.of(new ILiteralFragment(42L, BIGINT)))));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "extern func f(x: int)\n: int",            // the `:` of the result type stays on the line of the `)`
            "extern func f() { }",                     // a declaration has no body
            "extern f(x: int)",                        // no func keyword
            "extern func f(x: int, x: int)",           // the parameters have distinct names
            "extern func f(x: void)",                  // void is only a result type
            "extern func f()\nextern func f()",        // there is no overloading
    })
    void parseProgramInvalidFunctionDeclarationFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Nonnull
    static Stream<Arguments> parametersWithLabels() {
        return Stream.of(
                // as in Swift, the label of a parameter is its name, unless the parameter states a label before its
                // name; the label _ means no label, but in backticks it is an ordinary label
                Arguments.of("x: int", new IFunctionDefinitionFragment.Parameter("x", "x", INT)),
                Arguments.of("to x: int", new IFunctionDefinitionFragment.Parameter("to", "x", INT)),
                Arguments.of("_ x: int", new IFunctionDefinitionFragment.Parameter(null, "x", INT)),
                Arguments.of("_: int", new IFunctionDefinitionFragment.Parameter(null, "_", INT)),
                Arguments.of("`_` x: int", new IFunctionDefinitionFragment.Parameter("_", "x", INT)),
                // the label and the name can be on different lines
                Arguments.of("to\n    x: int", new IFunctionDefinitionFragment.Parameter("to", "x", INT)));
    }

    @ParameterizedTest
    @MethodSource("parametersWithLabels")
    void parseProgramParameterHasLabel(@Nonnull final String parameter,
                                       @Nonnull final IFunctionDefinitionFragment.Parameter expected) {
        final IFunctionDeclarationFragment f =
                (IFunctionDeclarationFragment)parseProgram("extern func f(" + parameter + ")").statements().get(0);
        assertThat(f.parameters()).containsExactly(expected);
    }

    @Test
    void parseProgramCallWritesLabelsOfParameters() {
        final IProgramFragment program = parseProgram("""
                extern func range<K>(closed: K, open: K): bigint
                extern func inc(_ x: int): int
                func move(to x: int): int { x }
                let r = range(closed: 1, open: 2)
                let i = inc(1)
                let m = move(
                    to:
                        3
                )""");
        final IFunctionDeclarationFragment range = (IFunctionDeclarationFragment)program.statements().get(0);
        final IFunctionDeclarationFragment inc = (IFunctionDeclarationFragment)program.statements().get(1);
        final IFunctionDefinitionFragment move = (IFunctionDefinitionFragment)program.statements().get(2);
        // the body of move uses the name x of its parameter, and the calls do not record the labels
        assertThat(move.body().statements()).containsExactly(new IVariableReferenceFragment("x", INT));
        assertThat(program.statements().subList(3, 6)).containsExactly(
                new IAssignmentFragment("r", new IFunctionCallFragment(new IFunctionReferenceFragment(range,
                        ImmutableList.of(INT), new IFunctionType(false, ImmutableList.of(INT, INT), BIGINT)),
                        ImmutableList.of(new ILiteralFragment(1, INT), new ILiteralFragment(2, INT)))),
                new IAssignmentFragment("i", new IFunctionCallFragment(
                        new IFunctionReferenceFragment(inc, ImmutableList.of(), inc.type()),
                        ImmutableList.of(new ILiteralFragment(1, INT)))),
                new IAssignmentFragment("m", new IFunctionCallFragment(
                        new IFunctionReferenceFragment(move, ImmutableList.of(), move.type()),
                        ImmutableList.of(new ILiteralFragment(3, INT)))));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // a call writes the label of each parameter, in the order of the parameters
            "extern func f(x: int)\nf(1)",
            "extern func f(x: int)\nf(y: 1)",
            "extern func f(x: int, y: int)\nf(y: 1, x: 2)",
            "extern func f(to x: int)\nf(x: 1)",
            // and no label for a parameter without a label, also not _
            "extern func f(_ x: int)\nf(x: 1)",
            "extern func f(_ x: int)\nf(_: 1)",
            // the label is not a name in the body
            "func f(to x: int): int { to }",
            // a dot call writes the labels of the parameters after the first one
            "extern func f(x: int, y: int): int\nlet e = 1.f(2)",
            // a function value has no labels, because a function type has none
            "extern func g(x: int): int\nlet h = g\nlet e = h(x: 1)",
            "let e = { (x: int) -> int in x }(x: 1)",
            // a closure parameter has no label
            "let e: (int) -> int = { (to x: int) in x }",
            // labels do not make overloading possible
            "extern func f(x: int)\nextern func f(y: int)",
            // the parameters still have distinct names
            "extern func f(a x: int, b x: int)",
    })
    void parseProgramInvalidLabelFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Nonnull
    static Stream<Arguments> validBlocks() {
        final IFunctionDefinitionFragment a = function("a");
        final IFunctionDefinitionFragment b = function("b");
        return Stream.of(
                // statements on one line are separated by semicolons, statements on separate lines by line breaks
                Arguments.of("func outer() { func a() { }; func b() { } }", function("outer", ImmutableList.of(a, b))),
                Arguments.of("func outer() {\n    func a() { }\n    func b() { }\n}", function("outer", ImmutableList.of(a, b))),
                Arguments.of("func outer() {\n    func a() { }; func b() { }\n}", function("outer", ImmutableList.of(a, b))),
                Arguments.of("func outer() {\r\n    func a() { }\r\n    func b() { }\r\n}", function("outer", ImmutableList.of(a, b))),
                // a semicolon after a statement, blank lines, and repeated semicolons are allowed
                Arguments.of("func outer() { func a() { }; }", function("outer", ImmutableList.of(a))),
                Arguments.of("func outer() {\n\n    ;; func a() { };\n\n    func b() { }\n\n}", function("outer", ImmutableList.of(a, b))),
                // the line break after a line comment still separates statements
                Arguments.of("func outer() {\n    func a() { } // the first\n    func b() { }\n}",
                        function("outer", ImmutableList.of(a, b))),
                // statements nest
                Arguments.of("func outer() {\n    func a() {\n        func b() { }\n    }\n}",
                        function("outer", ImmutableList.of(function("a", ImmutableList.of(b))))),
                // a closure on its own is a statement, as every expression is
                Arguments.of("func outer() { { } }", function("outer",
                        ImmutableList.of(new IClosureFragment(ImmutableList.of(), VOID, EMPTY_BLOCK)))));
    }

    @ParameterizedTest
    @MethodSource("validBlocks")
    void parseProgramStatementsInBodyCreateStatements(@Nonnull final String text,
                                                      @Nonnull final IFunctionDefinitionFragment expected) {
        assertThat(parseProgram(text).statements()).containsExactly(expected);
    }

    @Test
    void parseProgramStatementSplitAcrossLinesIsOneStatement() {
        final IProgramFragment program = parseProgram("""
                func c(x: int,
                       y: int)
                    : int
                {
                    x
                }
                func d<
                    T
                >(
                    xs: record<
                        id(1): bigint,
                        name: string?
                    >
                ) { }""");
        assertThat(program.statements()).containsExactly(
                new IFunctionDefinitionFragment("c", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", INT),
                                new IFunctionDefinitionFragment.Parameter("y", INT)),
                        INT, new IBlockFragment(ImmutableList.of(new IVariableReferenceFragment("x", INT)))),
                new IFunctionDefinitionFragment("d", ImmutableList.of(T),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("xs",
                                new IRecordType(false, ImmutableList.of(
                                        IRecordType.Field.of(BIGINT, "id", 1),
                                        IRecordType.Field.of(STRING.withNullability(true), "name"))))),
                        VOID, EMPTY_BLOCK));
    }

    @Test
    void parseProgramNestedFunctionSeesEnclosingTypeParameters() {
        final IProgramFragment program = parseProgram("""
                func outer<T>(x: T) {
                    func inner<U>(y: U, z: [T]): [T] { z }
                }""");
        final IArrayType arrayOfT = new IArrayType(false, T);
        assertThat(program.statements()).containsExactly(
                new IFunctionDefinitionFragment("outer", ImmutableList.of(T),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", T)), VOID,
                        new IBlockFragment(ImmutableList.of(new IFunctionDefinitionFragment("inner",
                                ImmutableList.of(U), ImmutableList.of(new IFunctionDefinitionFragment.Parameter("y", U),
                                        new IFunctionDefinitionFragment.Parameter("z", arrayOfT)), arrayOfT,
                                new IBlockFragment(ImmutableList.of(
                                        new IVariableReferenceFragment("z", arrayOfT))))))));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // statements on one line need a semicolon between them
            "func outer() { func a() { } func b() { } }",
            // a type parameter is in scope only within its function
            "func outer() { func a<U>() { }; func b(x: U) { } }",
            // the `?` of a nullable type stays on the line of its type
            "func f(x: int\n?) { }",
    })
    void parseProgramInvalidBlockFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Nonnull
    static Stream<Arguments> validStreamFunctions() {
        final IStreamType streamOfInt = new IStreamType(INT);
        final IFunctionDefinitionFragment one = new IFunctionDefinitionFragment("one", ImmutableList.of(),
                ImmutableList.of(), INT, new IBlockFragment(ImmutableList.of(new ILiteralFragment(1, INT))));
        return Stream.of(
                // a function returns the stream that its stream block constructs; the result type of the function is
                // the context of the stream block, and so of the values that the block yields
                Arguments.of("func numbers(): stream<bigint> {\n    stream {\n        yield 1\n"
                                + "        yield 2\n    }\n}",
                        function("numbers", new IStreamType(BIGINT), ImmutableList.of(new IStreamBlockFragment(BIGINT,
                                new IBlockFragment(ImmutableList.of(
                                        new IYieldFragment(new ILiteralFragment(1L, BIGINT)),
                                        new IYieldFragment(new ILiteralFragment(2L, BIGINT)))))))),
                // the code before the stream block runs when the function is called, and the block sees its variables
                Arguments.of("func twice<T>(x: T): stream<T> {\n    let y = x\n"
                                + "    stream {\n        yield x\n        yield y\n    }\n}",
                        new IFunctionDefinitionFragment("twice", ImmutableList.of(T),
                                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", T)), new IStreamType(T),
                                new IBlockFragment(ImmutableList.of(
                                        new IAssignmentFragment("y", new IVariableReferenceFragment("x", T)),
                                        new IStreamBlockFragment(T, new IBlockFragment(ImmutableList.of(
                                                new IYieldFragment(new IVariableReferenceFragment("x", T)),
                                                new IYieldFragment(new IVariableReferenceFragment("y", T))))))))),
                // a yield can be split across lines after its keyword
                Arguments.of("func numbers(): stream<int> {\n    stream {\n        yield\n            1\n    }\n}",
                        function("numbers", streamOfInt, ImmutableList.of(new IStreamBlockFragment(INT,
                                new IBlockFragment(ImmutableList.of(
                                        new IYieldFragment(new ILiteralFragment(1, INT)))))))),
                // a function in a stream block can compute what the block yields
                Arguments.of("func numbers(): stream<int> {\n    stream {\n        func one(): int { 1 }\n"
                                + "        yield one()\n    }\n}",
                        function("numbers", streamOfInt, ImmutableList.of(new IStreamBlockFragment(INT,
                                new IBlockFragment(ImmutableList.of(one, new IYieldFragment(new IFunctionCallFragment(
                                        new IFunctionReferenceFragment(one, ImmutableList.of(), one.type()),
                                        ImmutableList.of())))))))),
                // a closure can return a stream block
                Arguments.of("func numbers(): () -> stream<int> { { stream { yield 1 } } }", function("numbers",
                        new IFunctionType(false, ImmutableList.of(), streamOfInt),
                        ImmutableList.of(new IClosureFragment(ImmutableList.of(), streamOfInt,
                                new IBlockFragment(ImmutableList.of(new IStreamBlockFragment(INT,
                                        new IBlockFragment(ImmutableList.of(
                                                new IYieldFragment(new ILiteralFragment(1, INT))))))))))));
    }

    @ParameterizedTest
    @MethodSource("validStreamFunctions")
    void parseProgramFunctionReturnsStreamOfItsStreamBlock(@Nonnull final String text,
                                                           @Nonnull final IFunctionDefinitionFragment expected) {
        assertThat(parseProgram(text).statements()).containsExactly(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // a yield is a statement of a stream block
            "yield 1",
            "func f(): stream<int> { yield 1 }",
            "func f() { yield 1 }",
            "let e = { yield 1 }",
            // also within a stream block, a function or a closure cannot yield, since it can be called when the block
            // is not running
            "func f(): stream<int> {\n    stream {\n        func g() { yield 1 }\n        yield 2\n    }\n}",
            "extern func each(f: () -> void)\nfunc f(): stream<int> {\n    stream {\n"
                    + "        each { yield 1 }\n    }\n}",
            // a yield is a statement with one value
            "let e = stream { yield }",
            "let e = stream { yield 1, 2 }",
            "let e = stream { let x = yield 1 }",
            "extern func g(x: int)\nlet e = stream { g(yield 1) }",
            "let e = stream { yield 1 yield 2 }",           // statements on one line need a semicolon between them
            // a stream block has no parameters: the yields add to its stream
            "let e = stream { downStream in yield 1 }",
            // the function returns the stream, so its result type must be the type of the stream
            "func f(): int {\n    stream {\n        yield 1\n    }\n}",
            "func f(): stream<int> {\n    stream {\n        yield \"a\"\n    }\n}",
    })
    void parseProgramInvalidYieldFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Nonnull
    static Stream<Arguments> validConditionals() {
        final IVariableReferenceFragment c = new IVariableReferenceFragment("c", BOOLEAN);
        final IBlockFragment one = block(ImmutableList.of(new ILiteralFragment(1, INT)));
        final IBlockFragment two = block(ImmutableList.of(new ILiteralFragment(2, INT)));
        final IFunctionDeclarationFragment not = new IFunctionDeclarationFragment("not", ImmutableList.of(),
                ImmutableList.of(parameter("b", BOOLEAN)), BOOLEAN);
        final IIfFragment ifNotC = new IIfFragment(new IFunctionCallFragment(
                new IFunctionReferenceFragment(not, ImmutableList.of(), not.type()), ImmutableList.of(c)), one, null);
        return Stream.of(
                // an if runs its block if its condition is true, and the block after else, if there is one, otherwise
                Arguments.of("func f(c: boolean) { if c { 1 } }", new IIfFragment(c, one, null)),
                Arguments.of("func f(c: boolean) { if c { 1 } else { 2 } }", new IIfFragment(c, one, two)),
                // the else may stand on the line after the }, since a statement cannot start with it
                Arguments.of("func f(c: boolean) {\n    if c {\n        1\n    }\n    else {\n        2\n    }\n}",
                        new IIfFragment(c, one, two)),
                // an else if is an else block whose only statement is an if
                Arguments.of("func f(c: boolean, d: boolean) { if c { 1 } else if d { 2 } }",
                        new IIfFragment(c, one, block(ImmutableList.of(
                                new IIfFragment(new IVariableReferenceFragment("d", BOOLEAN), two, null))))),
                // the condition is an expression whose calls take no trailing closure; the block may start on the next
                // line, since the if cannot end before it
                Arguments.of("extern func not(b: boolean): boolean\nfunc f(c: boolean) { if not(b: c) { 1 } }", ifNotC),
                Arguments.of("extern func not(b: boolean): boolean\nfunc f(c: boolean) { if c.not()\n{ 1 } }", ifNotC),
                // in a function body, the statements of the blocks are typed one by one, as the statements around the
                // if are, so 42 has the default type of its literal
                Arguments.of("func f(c: boolean) { if c { let a = 42; a } }",
                        new IIfFragment(c, block(ImmutableList.of(
                                new IAssignmentFragment("a", new ILiteralFragment(42, INT)),
                                new IVariableReferenceFragment("a", INT))), null)));
    }

    @ParameterizedTest
    @MethodSource("validConditionals")
    void parseProgramIfCreatesIf(@Nonnull final String text, @Nonnull final IIfFragment expected) {
        assertThat(lastStatementOfLastFunction(text)).isEqualTo(expected);
    }

    @Nonnull
    static Stream<Arguments> validForLoops() {
        final IArrayType arrayOfInt = new IArrayType(false, INT);
        final IForFragment forXInXs = new IForFragment("x", new IVariableReferenceFragment("xs", arrayOfInt),
                block(ImmutableList.of(new IVariableReferenceFragment("x", INT))));
        final IType nullableString = STRING.withNullability(true);
        final IFunctionDeclarationFragment numbers = new IFunctionDeclarationFragment("numbers", ImmutableList.of(T),
                ImmutableList.of(parameter("x", T)), new IArrayType(false, T));
        return Stream.of(
                // a for loop runs its block for each element of an array, which is the value of the loop variable
                Arguments.of("func f(xs: [int]) { for x in xs { x } }", forXInXs),
                // a loop can be split across lines wherever it cannot end
                Arguments.of("func f(xs: [int]) {\n    for\n    x\n    in\n    xs\n    {\n        x\n    }\n}",
                        forXInXs),
                // the elements can be nullable, and the elements of an array of arrays are arrays
                Arguments.of("func f(xs: [string?]) { for x in xs { x } }", new IForFragment("x",
                        new IVariableReferenceFragment("xs", new IArrayType(false, nullableString)),
                        block(ImmutableList.of(new IVariableReferenceFragment("x", nullableString))))),
                Arguments.of("func f(xss: [[int]]) { for xs in xss { for x in xs { x } } }", new IForFragment("xs",
                        new IVariableReferenceFragment("xss", new IArrayType(false, arrayOfInt)),
                        block(ImmutableList.of(forXInXs)))),
                // the array is inferred before the loop variable is declared, so its name refers to what it stood for
                // before the loop
                Arguments.of("func f(x: [int]) { for x in x { x } }",
                        new IForFragment("x", new IVariableReferenceFragment("x", arrayOfInt),
                                block(ImmutableList.of(new IVariableReferenceFragment("x", INT))))),
                // in a function body, the array is typed before the statements of the block, which are typed one by
                // one, so the type of the loop variable is the one that the array has on its own
                Arguments.of("extern func numbers<T>(x: T): [T]\nfunc f() { for x in numbers(x: 1) { x } }",
                        new IForFragment("x", new IFunctionCallFragment(new IFunctionReferenceFragment(numbers,
                                ImmutableList.of(INT), new IFunctionType(false, ImmutableList.of(INT), arrayOfInt)),
                                ImmutableList.of(new ILiteralFragment(1, INT))),
                                block(ImmutableList.of(new IVariableReferenceFragment("x", INT))))));
    }

    @ParameterizedTest
    @MethodSource("validForLoops")
    void parseProgramForCreatesFor(@Nonnull final String text, @Nonnull final IForFragment expected) {
        assertThat(lastStatementOfLastFunction(text)).isEqualTo(expected);
    }

    @Nonnull
    static Stream<Arguments> validConsumes() {
        final IVariableReferenceFragment s = new IVariableReferenceFragment("s", new IStreamType(INT));
        final IVariableReferenceFragment reason = new IVariableReferenceFragment("reason", STOP_REASON);
        final IVariableReferenceFragment c = new IVariableReferenceFragment("c", BOOLEAN);
        final IYieldFragment yieldX = new IYieldFragment(new IVariableReferenceFragment("x", INT));
        final IConsumeFragment.Case valueX = new IConsumeFragment.Case("x", block(ImmutableList.of(yieldX)));
        final IConsumeFragment.Case stopForReason =
                new IConsumeFragment.Case("reason", block(ImmutableList.of(new IYieldBreakFragment(reason))));
        return Stream.of(
                // the value case runs for every element; the stopped case communicates the reason why the consumed
                // stream stopped outward, as the reason why the stream block stops
                Arguments.of("consume s {\n    case .value(x):\n        yield x\n"
                                + "    case .stopped(reason):\n        yield break reason\n}",
                        new IConsumeFragment(s, valueX, null, stopForReason)),
                // the end case runs at the end of the stream
                Arguments.of("consume s { case .value(x): yield x; case .end: yield break; "
                                + "case .stopped(reason): yield break reason }",
                        new IConsumeFragment(s, valueX, new IConsumeFragment.Case(null,
                                block(ImmutableList.of(new IYieldBreakFragment(null)))), stopForReason)),
                // the cases can come in any order, and a value or an end case can have no statements: a case does not
                // fall through
                Arguments.of("consume s {\n    case .end:\n    case .stopped(reason):\n        yield break reason\n"
                                + "    case .value(x):\n        yield x\n}",
                        new IConsumeFragment(s, valueX, new IConsumeFragment.Case(null, EMPTY_BLOCK), stopForReason)),
                // a reason stays on the line of its yield break: on the next line, it is a statement of its own
                Arguments.of("consume s {\n    case .value(x):\n        yield x\n"
                                + "    case .stopped(reason):\n        yield break\n        reason\n"
                                + "        yield break reason\n}",
                        new IConsumeFragment(s, valueX, null, new IConsumeFragment.Case("reason",
                                block(ImmutableList.of(new IYieldBreakFragment(null), reason,
                                        new IYieldBreakFragment(reason)))))),
                // the stopped case ends with a yield break on every path, here in both blocks of an if, of which one
                // ends the stream instead of stopping it
                Arguments.of("consume s { case .value(x): yield x; "
                                + "case .stopped(reason): if c { yield break reason } else { yield break } }",
                        new IConsumeFragment(s, valueX, null, new IConsumeFragment.Case("reason",
                                block(ImmutableList.of(new IIfFragment(c,
                                        block(ImmutableList.of(new IYieldBreakFragment(reason))),
                                        block(ImmutableList.of(new IYieldBreakFragment(null))))))))),
                // a yield in a block of an if in a case belongs to the stream block too
                Arguments.of("consume s { case .value(x): if c { yield x }; "
                                + "case .stopped(reason): yield break reason }",
                        new IConsumeFragment(s, new IConsumeFragment.Case("x", block(ImmutableList.of(
                                new IIfFragment(c, block(ImmutableList.of(yieldX)), null)))), null, stopForReason)),
                // the consumed stream can be any expression of a stream type, a stream block included
                Arguments.of("consume stream { yield 1 } { case .value(x): yield x; "
                                + "case .stopped(reason): yield break reason }",
                        new IConsumeFragment(new IStreamBlockFragment(INT, block(ImmutableList.of(
                                new IYieldFragment(new ILiteralFragment(1, INT))))), valueX, null, stopForReason)));
    }

    @ParameterizedTest
    @MethodSource("validConsumes")
    void parseProgramConsumeCreatesConsume(@Nonnull final String consume, @Nonnull final IConsumeFragment expected) {
        // a consume is a statement of a stream block
        final IStatementFragment streamBlock = lastStatementOfLastFunction(
                "func f(s: stream<int>, c: boolean): stream<int> {\n    stream {\n" + consume + "\n    }\n}");
        final List<IStatementFragment> statements = ((IStreamBlockFragment)streamBlock).body().statements();
        assertThat(statements.get(statements.size() - 1)).isEqualTo(expected);
    }

    @Test
    void parseProgramFlatmapStopsAtStopOfInnerStream() {
        final IProgramFragment program = parseProgram("""
                func flatmap<T, R>(outer: stream<T>, innerProducer: (T) -> stream<R>): stream<R> {
                    stream {
                        consume outer {
                            case .value(nextOuter):
                                let inner = innerProducer(nextOuter)
                                consume inner {
                                    case .value(nextInner):
                                        yield nextInner
                                    case .stopped(reason):
                                        yield break reason
                                }
                            case .stopped(reason):
                                yield break reason
                        }
                    }
                }""");
        final IGenericType r = new IGenericType("R", false);
        final IFunctionType producerType = new IFunctionType(false, ImmutableList.of(T), new IStreamType(r));
        final IConsumeFragment.Case stopForReason = new IConsumeFragment.Case("reason", block(ImmutableList.of(
                new IYieldBreakFragment(new IVariableReferenceFragment("reason", STOP_REASON)))));
        // an inner stop stops the flatmap, and the end of the inner stream leaves the inner loop only
        final IConsumeFragment innerConsume = new IConsumeFragment(
                new IVariableReferenceFragment("inner", new IStreamType(r)),
                new IConsumeFragment.Case("nextInner", block(ImmutableList.of(
                        new IYieldFragment(new IVariableReferenceFragment("nextInner", r))))),
                null, stopForReason);
        final IConsumeFragment outerConsume = new IConsumeFragment(
                new IVariableReferenceFragment("outer", new IStreamType(T)),
                new IConsumeFragment.Case("nextOuter", block(ImmutableList.of(
                        new IAssignmentFragment("inner", new IFunctionCallFragment(
                                new IVariableReferenceFragment("innerProducer", producerType),
                                ImmutableList.of(new IVariableReferenceFragment("nextOuter", T)))),
                        innerConsume))),
                null, stopForReason);
        assertThat(program.statements()).containsExactly(new IFunctionDefinitionFragment("flatmap",
                ImmutableList.of(T, r),
                ImmutableList.of(parameter("outer", new IStreamType(T)), parameter("innerProducer", producerType)),
                new IStreamType(r), block(ImmutableList.of(
                        new IStreamBlockFragment(r, block(ImmutableList.of(outerConsume)))))));
    }

    @Test
    void parseProgramExplodeYieldsElementsOfArray() {
        final IProgramFragment program = parseProgram("""
                func explode<T>(a: [T]): stream<T> {
                    stream {
                        for t in a {
                            yield t
                        }
                    }
                }""");
        final IArrayType arrayOfT = new IArrayType(false, T);
        assertThat(program.statements()).containsExactly(new IFunctionDefinitionFragment("explode",
                ImmutableList.of(T), ImmutableList.of(parameter("a", arrayOfT)), new IStreamType(T),
                block(ImmutableList.of(new IStreamBlockFragment(T, block(ImmutableList.of(new IForFragment("t",
                        new IVariableReferenceFragment("a", arrayOfT),
                        block(ImmutableList.of(new IYieldFragment(new IVariableReferenceFragment("t", T))))))))))));
    }

    @Test
    void parseProgramStopReasonIsAliasOfEnumThatFrlIrDeclares() {
        final IFunctionDeclarationFragment timeout = new IFunctionDeclarationFragment("timeout", ImmutableList.of(),
                ImmutableList.of(), STOP_REASON);
        final IProgramFragment expected = new IProgramFragment(ImmutableList.of(timeout, function("f",
                new IStreamType(INT), ImmutableList.of(new IStreamBlockFragment(INT, block(ImmutableList.of(
                        new IYieldBreakFragment(new IFunctionCallFragment(new IFunctionReferenceFragment(timeout,
                                ImmutableList.of(), timeout.type()), ImmutableList.of())))))))));
        // a stream block without yields takes its element type from its context
        assertThat(parseProgram("extern func timeout(): stopReason\n"
                + "func f(): stream<int> { stream { yield break timeout() } }")).isEqualTo(expected);
        // the alias stands for the enum, which a program can also spell out
        assertThat(parseProgram("extern func timeout(): enum<RETURN_LIMIT_REACHED = 1, TIME_LIMIT_REACHED = 2, "
                + "SCAN_LIMIT_REACHED = 3, BYTE_LIMIT_REACHED = 4>\n"
                + "func f(): stream<int> { stream { yield break timeout() } }")).isEqualTo(expected);
    }

    @Test
    void parseProgramStatementsOfControlFlowInClosureJoinInferenceOfClosure() {
        // in a closure, the statements of the blocks of an if and a for join the inference of the statement that the
        // closure is in, as the other statements of the closure do: a later statement can still make 42 a bigint, and
        // the body of a for loop can determine the element type of its array
        final IProgramFragment program = parseProgram("""
                extern func h(x: bigint)
                extern func numbers<T>(x: T): [T]
                extern func run(f: () -> void)
                func f(c: boolean) {
                    run {
                        if c {
                            let a = 42
                            h(x: a)
                        }
                        for x in numbers(x: 1) {
                            h(x: x)
                        }
                    }
                }""");
        final IFunctionDeclarationFragment h = (IFunctionDeclarationFragment)program.statements().get(0);
        final IFunctionDeclarationFragment numbers = (IFunctionDeclarationFragment)program.statements().get(1);
        final IFunctionDeclarationFragment run = (IFunctionDeclarationFragment)program.statements().get(2);
        final IFunctionReferenceFragment hReference = new IFunctionReferenceFragment(h, ImmutableList.of(), h.type());
        final IClosureFragment closure = new IClosureFragment(ImmutableList.of(), VOID, block(ImmutableList.of(
                new IIfFragment(new IVariableReferenceFragment("c", BOOLEAN), block(ImmutableList.of(
                        new IAssignmentFragment("a", new ILiteralFragment(42L, BIGINT)),
                        new IFunctionCallFragment(hReference,
                                ImmutableList.of(new IVariableReferenceFragment("a", BIGINT))))), null),
                new IForFragment("x", new IFunctionCallFragment(new IFunctionReferenceFragment(numbers,
                        ImmutableList.of(BIGINT),
                        new IFunctionType(false, ImmutableList.of(BIGINT), new IArrayType(false, BIGINT))),
                        ImmutableList.of(new ILiteralFragment(1L, BIGINT))),
                        block(ImmutableList.of(new IFunctionCallFragment(hReference,
                                ImmutableList.of(new IVariableReferenceFragment("x", BIGINT)))))))));
        assertThat(((IFunctionDefinitionFragment)program.statements().get(3)).body().statements()).containsExactly(
                new IFunctionCallFragment(new IFunctionReferenceFragment(run, ImmutableList.of(), run.type()),
                        ImmutableList.of(closure)));
    }

    @Test
    void parseProgramStatementsOfControlFlowInStreamBlockJoinInferenceOfStreamBlock() {
        // in a stream block, the statements of the blocks of an if join the inference of the statement that the stream
        // block is in, so a yield can make 42 a value of the element type
        final IProgramFragment program = parseProgram("""
                func f(c: boolean): stream<bigint> {
                    stream {
                        if c {
                            let a = 42
                            yield a
                        }
                    }
                }""");
        assertThat(program.statements()).containsExactly(new IFunctionDefinitionFragment("f", ImmutableList.of(),
                ImmutableList.of(parameter("c", BOOLEAN)), new IStreamType(BIGINT), block(ImmutableList.of(
                        new IStreamBlockFragment(BIGINT, block(ImmutableList.of(new IIfFragment(
                                new IVariableReferenceFragment("c", BOOLEAN), block(ImmutableList.of(
                                        new IAssignmentFragment("a", new ILiteralFragment(42L, BIGINT)),
                                        new IYieldFragment(new IVariableReferenceFragment("a", BIGINT)))),
                                null))))))));
    }

    @Test
    void parseProgramControlFlowIsStatementOfProgram() {
        final IFunctionDeclarationFragment xs = new IFunctionDeclarationFragment("xs", ImmutableList.of(),
                ImmutableList.of(), new IArrayType(false, INT));
        assertThat(parseProgram("extern func xs(): [int]\nlet c = true\nif c { 1 }\nfor x in xs() { x }").statements())
                .containsExactly(xs, new IAssignmentFragment("c", new ILiteralFragment(true, BOOLEAN)),
                        new IIfFragment(new IVariableReferenceFragment("c", BOOLEAN),
                                block(ImmutableList.of(new ILiteralFragment(1, INT))), null),
                        new IForFragment("x", new IFunctionCallFragment(
                                new IFunctionReferenceFragment(xs, ImmutableList.of(), xs.type()), ImmutableList.of()),
                                block(ImmutableList.of(new IVariableReferenceFragment("x", INT)))));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // the condition of an if is a boolean that is not nullable
            "func f(x: int) { if x { } }",
            "func f(c: boolean?) { if c { } }",
            // an if has a block, and an else has a block or another if
            "func f(c: boolean) { if c }",
            "func f(c: boolean) { if c { } else }",
            "func f() { else { } }",
            // the expression in front of a block takes no trailing closure: there, a { starts the block
            "extern func run(f: () -> boolean): boolean\nfunc g() { if run { true } { } }",
            // a for loop iterates over an array that is not nullable, and not over a stream
            "func f(x: int) { for y in x { } }",
            "func f(xs: [int]?) { for x in xs { } }",
            "func f(s: stream<int>) { for x in s { } }",
            "func f(xs: [int]) { for x xs { } }",
            // the loop variable is in scope only in the body
            "func f(xs: [int]) { for x in xs { }; x }",
            // a consume consumes a stream, in a stream block, and not in a function or a closure within one
            "func f(s: stream<int>) { consume s { case .value(x): x } }",
            "func f(xs: [int]): stream<int> { stream { consume xs { case .value(x): yield x } } }",
            "func f(s: stream<int>): stream<int> {\n    stream {\n"
                    + "        let c = { () -> void in consume s { case .value(x): x } }\n        yield 1\n    }\n}",
            // a consume has one value case, which binds a name, at most one end case, which binds none, and one stopped
            // case, which binds one
            "func f(s: stream<int>): stream<int> {\n"
                    + "    stream { consume s { case .end: yield break; case .stopped(reason): yield break reason } }\n"
                    + "}",
            "func f(s: stream<int>): stream<int> {\n"
                    + "    stream { consume s { case .value(x): yield x; case .value(y): x } }\n}",
            "func f(s: stream<int>): stream<int> { stream { consume s { case .value: yield 1 } } }",
            "func f(s: stream<int>): stream<int> { stream { consume s { case .value(x): yield x; case .end(y): x } } }",
            "func f(s: stream<int>): stream<int> {\n"
                    + "    stream { consume s { case .value(x): yield x; case .stopped: x } }\n}",
            "func f(s: stream<int>): stream<int> { stream { consume s { case .value(x): yield x; case .done: x } } }",
            "func f(s: stream<int>): stream<int> { stream { consume s { } } }",
            // a binding is in scope only in its case
            "func f(s: stream<int>): stream<int> {\n"
                    + "    stream { consume s { case .value(x): yield x; case .end: yield x } }\n}",
            // the reason of a stop has to be communicated outward: every consume has a stopped case, whose statements
            // end with a yield break on every path
            "func f(s: stream<int>): stream<int> { stream { consume s { case .value(x): yield x } } }",
            "func f(s: stream<int>): stream<int> {\n"
                    + "    stream { consume s { case .value(x): yield x; case .end: yield break } }\n}",
            "func f(s: stream<int>): stream<int> {\n"
                    + "    stream { consume s { case .value(x): yield x; case .stopped(reason): } }\n}",
            "func f(s: stream<int>): stream<int> {\n"
                    + "    stream { consume s { case .value(x): yield x; case .stopped(reason): yield 1 } }\n}",
            "func f(s: stream<int>): stream<int> {\n    stream {\n"
                    + "        consume s { case .value(x): yield x; case .stopped(reason): yield break reason; "
                    + "yield 1 }\n    }\n}",
            "func f(s: stream<int>, c: boolean): stream<int> {\n    stream {\n"
                    + "        consume s { case .value(x): yield x; case .stopped(reason): "
                    + "if c { yield break reason } }\n    }\n}",
            "func f(s: stream<int>, xs: [int]): stream<int> {\n    stream {\n"
                    + "        consume s { case .value(x): yield x; case .stopped(reason): "
                    + "for y in xs { yield break } }\n    }\n}",
            "func f(s: stream<int>): stream<int> {\n    stream {\n"
                    + "        consume s {\n            case .value(x):\n                yield x\n"
                    + "            case .stopped(reason):\n                yield break\n                reason\n"
                    + "        }\n    }\n}",
            // a yield break is in a stream block, and its reason is a stopReason
            "yield break",
            "func f() { yield break }",
            "func f(): stream<int> { stream { yield break 1 } }",
            // the end of a stream is not a reason, so the enum of all the no-next reasons of a cursor is not a
            // stopReason
            "extern func e(): enum<SOURCE_EXHAUSTED = 0, RETURN_LIMIT_REACHED = 1, TIME_LIMIT_REACHED = 2, "
                    + "SCAN_LIMIT_REACHED = 3, BYTE_LIMIT_REACHED = 4>\n"
                    + "func f(): stream<int> { stream { yield break e() } }",
            "func f(): stream<int> {\n    stream {\n        let c = { () -> void in yield break }\n"
                    + "        yield 1\n    }\n}",
            // stopReason is a type alias that FRL-IR declares itself, in the scope of the statements of a program
            "extern type stopReason",
            "typealias stopReason = int",
            // a statement of control flow does not make a function a place for a yield
            "func f(c: boolean) { if c { yield 1 } }",
            // in a function body or a program, the statements of the blocks of an if or a for are typed one by one, as
            // the statements around the if or the for are, so a later statement cannot make 42 a bigint
            "extern func h(x: bigint)\nfunc f(c: boolean) { if c { let a = 42; h(x: a) } }",
            "extern func h(x: bigint)\nfunc f(c: boolean) { if c { } else { let a = 42; h(x: a) } }",
            "extern func h(x: bigint)\nfunc f(c: boolean, d: boolean) { if c { } else if d { let a = 42; h(x: a) } }",
            "extern func h(x: bigint)\nfunc f(xs: [int]) { for x in xs { let a = 42; h(x: a) } }",
            "extern func h(x: bigint)\nfunc f(xs: [int], c: boolean) { for x in xs { if c { let a = 42; h(x: a) } } }",
            "extern func h(x: bigint)\nlet c = true\nif c { let a = 42; h(x: a) }",
            // nor can the body of a for loop determine the element type of its array there
            "extern func numbers<T>(x: T): [T]\nextern func h(x: bigint)\n"
                    + "func f() { for x in numbers(x: 1) { h(x: x) } }",
            "extern func empty<T>(): [T]\nextern func h(x: bigint)\nfunc f() { for x in empty() { h(x: x) } }",
    })
    void parseProgramInvalidControlFlowFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Nonnull
    static Stream<Arguments> validAssignments() {
        final IAssignmentFragment bigintX = new IAssignmentFragment("x", new ILiteralFragment(42L, BIGINT));
        return Stream.of(
                // the type of the variable is the type of its expression
                Arguments.of("let x = 42", new IAssignmentFragment("x", new ILiteralFragment(42, INT))),
                Arguments.of("let x: bigint = 42", bigintX),
                // an assignment can be split across lines wherever it cannot end
                Arguments.of("let\nx\n:\nbigint\n=\n42", bigintX),
                // names between backticks
                Arguments.of("let `in` = 42", new IAssignmentFragment("in", new ILiteralFragment(42, INT))));
    }

    @ParameterizedTest
    @MethodSource("validAssignments")
    void parseProgramAssignmentCreatesAssignment(@Nonnull final String text,
                                                 @Nonnull final IAssignmentFragment expected) {
        assertThat(parseProgram(text).statements()).containsExactly(expected);
    }

    @Test
    void parseProgramExpressionIsStatement() {
        assertThat(parseProgram("let x = 42\nx\n-1\n{ }").statements()).containsExactly(
                new IAssignmentFragment("x", new ILiteralFragment(42, INT)),
                new IVariableReferenceFragment("x", INT),
                new ILiteralFragment(-1, INT),
                new IClosureFragment(ImmutableList.of(), VOID, EMPTY_BLOCK));
    }

    @Test
    void parseProgramVariableIsInScopeFromNextStatement() {
        assertThat(parseProgram("let x = 42; let y = x").statements()).containsExactly(
                new IAssignmentFragment("x", new ILiteralFragment(42, INT)),
                new IAssignmentFragment("y", new IVariableReferenceFragment("x", INT)));
    }

    @Test
    void parseProgramInnerVariableHidesOuterOne() {
        final IProgramFragment program = parseProgram("""
                let x = 42
                func f(x: string) {
                    let y = x
                    let x = true
                    let z = x
                }
                let w = x""");
        // in f, x is first the parameter and then the variable of the body; after f, it is the outer variable again
        assertThat(program.statements()).containsExactly(
                new IAssignmentFragment("x", new ILiteralFragment(42, INT)),
                new IFunctionDefinitionFragment("f", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", STRING)), VOID,
                        new IBlockFragment(ImmutableList.of(
                                new IAssignmentFragment("y", new IVariableReferenceFragment("x", STRING)),
                                new IAssignmentFragment("x", new ILiteralFragment(true, BOOLEAN)),
                                new IAssignmentFragment("z", new IVariableReferenceFragment("x", BOOLEAN))))),
                new IAssignmentFragment("w", new IVariableReferenceFragment("x", INT)));
    }

    @Test
    void parseProgramReferenceRefersToDefinition() {
        final IProgramFragment program = parseProgram("""
                func f(n: bigint): bigint {
                    let m = n
                    m
                }
                f(n: 42)""");
        final IFunctionDefinitionFragment f = new IFunctionDefinitionFragment("f", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("n", BIGINT)), BIGINT,
                new IBlockFragment(ImmutableList.of(
                        new IAssignmentFragment("m", new IVariableReferenceFragment("n", BIGINT)),
                        new IVariableReferenceFragment("m", BIGINT))));
        assertThat(program.statements()).containsExactly(f,
                // a call on its own is a statement
                new IFunctionCallFragment(new IFunctionReferenceFragment(f, ImmutableList.of(), f.type()),
                        ImmutableList.of(new ILiteralFragment(42L, BIGINT))));
        // the name is resolved once, while parsing: the reference holds the definition of the program
        final IFunctionCallFragment call = (IFunctionCallFragment)program.statements().get(1);
        assertThat(((IFunctionReferenceFragment)call.callee()).function()).isSameAs(program.statements().get(0));
    }

    @Test
    void parseProgramReferenceRefersToInnermostDefinition() {
        final IProgramFragment program = parseProgram("""
                func f(): int { 1 }
                func g(): int {
                    func f(): int { 2 }
                    f()
                }""");
        final IFunctionDefinitionFragment innerF = new IFunctionDefinitionFragment("f", ImmutableList.of(),
                ImmutableList.of(), INT, new IBlockFragment(ImmutableList.of(new ILiteralFragment(2, INT))));
        final IFunctionDefinitionFragment g = (IFunctionDefinitionFragment)program.statements().get(1);
        assertThat(g.body().statements()).containsExactly(innerF,
                new IFunctionCallFragment(new IFunctionReferenceFragment(innerF, ImmutableList.of(), innerF.type()),
                        ImmutableList.of()));
    }

    @Test
    void parseProgramExternTypeIsTypeOfItsDeclaration() {
        final IProgramFragment program = parseProgram("""
                extern type range
                extern func bounds(range: range, other: range?): [record<low: range, high: range?>]
                extern func merge(ranges: stream<range>, combine: (range, range) -> range): stream<range>""");
        final ITypeDeclarationFragment rangeDeclaration =
                new ITypeDeclarationFragment("range", ImmutableList.of(), null);
        final IExternType range = new IExternType(rangeDeclaration, false);
        assertThat(program.statements()).containsExactly(rangeDeclaration,
                new IFunctionDeclarationFragment("bounds", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("range", range),
                                new IFunctionDefinitionFragment.Parameter("other", range.withNullability(true))),
                        new IArrayType(false, new IRecordType(false, ImmutableList.of(
                                IRecordType.Field.of(range, "low"),
                                IRecordType.Field.of(range.withNullability(true), "high"))))),
                new IFunctionDeclarationFragment("merge", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("ranges", new IStreamType(range)),
                                new IFunctionDefinitionFragment.Parameter("combine",
                                        new IFunctionType(false, ImmutableList.of(range, range), range))),
                        new IStreamType(range)));
    }

    @Test
    void parseProgramValueOfExternTypePassesBetweenFunctions() {
        final IProgramFragment program = parseProgram("""
                extern type range
                extern func everything(): range
                extern func count(r: range): bigint
                let r = everything()
                let n = count(r: r)""");
        final IExternType range =
                new IExternType(new ITypeDeclarationFragment("range", ImmutableList.of(), null), false);
        assertThat(program.statements().get(3)).isEqualTo(new IAssignmentFragment("r", new IFunctionCallFragment(
                new IFunctionReferenceFragment((IFunctionDeclarationFragment)program.statements().get(1),
                        ImmutableList.of(), new IFunctionType(false, ImmutableList.of(), range)), ImmutableList.of())));
        assertThat(((IAssignmentFragment)program.statements().get(4)).expression().type()).isEqualTo(BIGINT);
    }

    @Test
    void parseProgramPackageAndAnnotationsAreKeptAsWritten() {
        final IProgramFragment program = parseProgram("""
                @java("com.apple.foundationdb.record.ir")
                @swift(module: "RecordLayerIR")
                package abc

                @java(class:
                    "com.apple.foundationdb.record.TupleRange")
                extern type range
                @java(method: "scanRecords") extern func scan(range: range): bigint
                @inline func id(x: int): int { x }""");
        final IPackageStatementFragment abc = new IPackageStatementFragment("abc", ImmutableList.of(
                annotation("java", new IAnnotationFragment.Argument(null, "com.apple.foundationdb.record.ir")),
                annotation("swift", new IAnnotationFragment.Argument("module", "RecordLayerIR"))));
        final ITypeDeclarationFragment range = new ITypeDeclarationFragment("range", ImmutableList.of(annotation("java",
                new IAnnotationFragment.Argument("class", "com.apple.foundationdb.record.TupleRange"))), abc);
        assertThat(program.packageStatement()).contains(abc);
        // the declarations of the program record its package
        assertThat(program.statements()).containsExactly(abc, range,
                new IFunctionDeclarationFragment("scan", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("range",
                                new IExternType(range, false))),
                        BIGINT, ImmutableList.of(annotation("java",
                                new IAnnotationFragment.Argument("method", "scanRecords"))), abc),
                new IFunctionDefinitionFragment("id", ImmutableList.of(), ImmutableList.of(
                        new IFunctionDefinitionFragment.Parameter("x", INT)), INT,
                        new IBlockFragment(ImmutableList.of(new IVariableReferenceFragment("x", INT))),
                        ImmutableList.of(new IAnnotationFragment("inline", ImmutableList.of()))));
    }

    @Test
    void parseProgramWithoutPackageHasNoPackage() {
        assertThat(parseProgram("extern type range").packageStatement()).isEmpty();
        assertThat(parseProgram("extern type range").statements())
                .containsExactly(new ITypeDeclarationFragment("range", ImmutableList.of(), null));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "extern type range\npackage abc",            // the package statement is the first statement
            "package abc\npackage def",                  // a program belongs to one package at most
            "func f() {\n    package abc\n}",            // also not in a function body
            "let e = { package abc }",                   // or in a closure
            "package",                                   // a package has a name
            "package abc def",
            "@java(42) package abc",                     // an annotation argument is a string
            "@java(class \"a.B\") extern type range",    // a label is followed by a colon
            "@java(\"a.B\")\nlet x = 1",                 // only declarations are annotated
            "@java(\"a.B\")",
    })
    void parseProgramInvalidPackageOrAnnotationFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "extern type range\nextern type range",              // a scope declares a type name only once
            "extern func f(r: range)\nextern type range",        // a type is in scope from its declaration on
            "func f() { extern type t }\nextern func g(x: t)",   // and only in its block
            "extern type range<T>",                              // an extern type has no type parameters
            "extern type",                                       // an extern type has a name
            "extern type int",
            "extern type range\nlet r: range = 42",              // a literal cannot be of an extern type
            "extern type range\nlet r: range? = 42",
            // two extern types are different types
            "extern type r\nextern type s\nextern func f(x: r)\nextern func g(): s\nf(x: g())",
    })
    void parseProgramInvalidExternTypeFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Nonnull
    static Stream<Arguments> validTypeAliases() {
        final IRecordType row = new IRecordType(false, ImmutableList.of(IRecordType.Field.of(BIGINT, "id"),
                IRecordType.Field.of(STRING.withNullability(true), "name")));
        return Stream.of(
                // an alias names a type of any kind, a nullable one included
                Arguments.of("typealias row = record<id: bigint, name: string?>", new ITypeAliasFragment("row", row)),
                Arguments.of("typealias name = string?", new ITypeAliasFragment("name", STRING.withNullability(true))),
                Arguments.of("typealias numbers = stream<int>",
                        new ITypeAliasFragment("numbers", new IStreamType(INT))),
                Arguments.of("typealias f = (int) -> bigint",
                        new ITypeAliasFragment("f", new IFunctionType(false, ImmutableList.of(INT), BIGINT))),
                // as in Swift, void can have another name, which is then only a result type
                Arguments.of("typealias nothing = void", new ITypeAliasFragment("nothing", VOID)),
                // an alias can be split across lines wherever it cannot end, and its name can be between backticks
                Arguments.of("typealias\nrow\n=\nrecord<id: bigint, name: string?>",
                        new ITypeAliasFragment("row", row)),
                Arguments.of("typealias `first row` = int", new ITypeAliasFragment("first row", INT)));
    }

    @ParameterizedTest
    @MethodSource("validTypeAliases")
    void parseProgramTypeAliasCreatesTypeAlias(@Nonnull final String text,
                                               @Nonnull final ITypeAliasFragment expected) {
        assertThat(parseProgram(text).statements()).containsExactly(expected);
    }

    @Test
    void parseProgramTypeAliasStandsForItsType() {
        final IProgramFragment program = parseProgram("""
                typealias row = record<id: bigint, name: string?>
                typealias rows = [row]
                extern func scan(): rows
                extern func first(rs: rows): row?
                let r = first(rs: scan())""");
        final IRecordType row = new IRecordType(false, ImmutableList.of(IRecordType.Field.of(BIGINT, "id"),
                IRecordType.Field.of(STRING.withNullability(true), "name")));
        final IArrayType rows = new IArrayType(false, row);
        final IFunctionDeclarationFragment scan =
                new IFunctionDeclarationFragment("scan", ImmutableList.of(), ImmutableList.of(), rows);
        final IFunctionDeclarationFragment first = new IFunctionDeclarationFragment("first", ImmutableList.of(),
                ImmutableList.of(parameter("rs", rows)), row.withNullability(true));
        // the aliases remain statements of the program, and a type that names an alias is the type of the alias, so
        // row? is the record type made nullable
        assertThat(program.statements()).containsExactly(new ITypeAliasFragment("row", row),
                new ITypeAliasFragment("rows", rows), scan, first,
                new IAssignmentFragment("r", new IFunctionCallFragment(
                        new IFunctionReferenceFragment(first, ImmutableList.of(), first.type()),
                        ImmutableList.of(new IFunctionCallFragment(
                                new IFunctionReferenceFragment(scan, ImmutableList.of(), scan.type()),
                                ImmutableList.of())))));
    }

    @Test
    void parseProgramInnerTypeAliasHidesOuterOne() {
        final IProgramFragment program = parseProgram("""
                typealias t = int
                func f<T>(xs: [T]) {
                    typealias t = [T]
                    let ys: t = xs
                }
                let t: t = 1""");
        final IArrayType arrayOfT = new IArrayType(false, T);
        // in f, t is the alias of its body, which can name a type parameter of f; after f, t is the outer alias again
        assertThat(program.statements()).containsExactly(new ITypeAliasFragment("t", INT),
                new IFunctionDefinitionFragment("f", ImmutableList.of(T), ImmutableList.of(parameter("xs", arrayOfT)),
                        VOID, block(ImmutableList.of(new ITypeAliasFragment("t", arrayOfT),
                                new IAssignmentFragment("ys", new IVariableReferenceFragment("xs", arrayOfT))))),
                // types and values have separate namespaces, so a variable can have the name of a type alias
                new IAssignmentFragment("t", new ILiteralFragment(1, INT)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "typealias row",                                                // an alias has a type
            "typealias = int",                                              // and a name
            "typealias row =",
            "typealias row: int",                                           // the type follows an `=`
            "typealias int = bigint",                                       // a keyword is not a name
            "typealias row<T> = [T]",                                       // an alias has no type parameters
            "typealias row = row",                                          // an alias is in scope from the next
            "typealias row = [row]",                                        // statement on, so its type cannot refer
            "extern func f(): row\ntypealias row = int",                    // to it
            "func f() { typealias t = int }\nextern func g(x: t)",          // and only in its block
            "typealias t = u",                                              // the type is in scope
            "typealias t = int\ntypealias t = int",                         // a scope declares a type name only once,
            "extern type t\ntypealias t = int",                             // also among extern types and aliases
            "typealias t = int\nextern type t",
            "typealias name = string?\nextern func f(): name?",             // a type cannot be made nullable twice,
            "typealias numbers = stream<int>\nextern func f(): numbers?",   // and a stream type never
            "typealias nothing = void\nextern func f(x: nothing)",          // void is only a result type
            "typealias nothing = void\nextern func f(): nothing?",
            "typealias t = int\nlet x: t = \"a\"",                          // a value of t is a value of its type
    })
    void parseProgramInvalidTypeAliasFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "let x",                                     // no expression
            "let = 42",                                  // no name
            "let x =",                                   // no expression after the `=`
            "let x = 42 let y = 43",                     // statements on one line need a semicolon between them
            "f(1) g(2)",                                 // also when they are expressions
            "{ } { }",
            "let x = x",                                 // the variable is not in scope in its own expression
            "let x = 1\nlet x = 2",                      // a scope declares a name only once
            "func f() { }\nlet f = 1",                   // variables and functions share their names
            "func f() { }\nfunc f() { }",                // there is no overloading
            "func f(x: int, x: int) { }",                // the parameters have distinct names
            "let y = f(1)\nfunc f(x: int): int { x }",   // a function is in scope from its definition on
            "func f(n: int): int { f(n) }",              // so it cannot call itself: FRL-IR has no recursion
            "func f() { func g() { f() } }",             // not even from a nested function
            "func f() { let x = 1 }\nlet y = x",         // a variable is in scope only in its block
            "func f(x: int) { }\nf\n(1)",                // a `(` on the next line cannot start a statement
            "func outer<T>() { func inner<T>() { } }",   // a type parameter must not hide one of an enclosing function
            "extern func g()\nlet x = g()",              // a void call has no value to name
            "extern func g()\nlet x: void = g()",
    })
    void parseProgramInvalidStatementFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Returns a function definition without parameters and statements that returns {@code void}.
     */
    @Nonnull
    private static IFunctionDefinitionFragment function(@Nonnull final String name) {
        return function(name, ImmutableList.of());
    }

    /**
     * Returns a function definition without parameters that returns {@code void} and whose body holds the given
     * statements.
     */
    @Nonnull
    private static IFunctionDefinitionFragment function(@Nonnull final String name,
                                                        @Nonnull final List<IStatementFragment> statements) {
        return new IFunctionDefinitionFragment(name, ImmutableList.of(), ImmutableList.of(), VOID,
                new IBlockFragment(statements));
    }

    /**
     * Returns a function definition without parameters of the given result type whose body holds the given statements.
     */
    @Nonnull
    private static IFunctionDefinitionFragment function(@Nonnull final String name, @Nonnull final IType resultType,
                                                        @Nonnull final List<IStatementFragment> statements) {
        return new IFunctionDefinitionFragment(name, ImmutableList.of(), ImmutableList.of(), resultType,
                new IBlockFragment(statements));
    }

    @Nonnull
    private static IAnnotationFragment annotation(@Nonnull final String name,
                                                  @Nonnull final IAnnotationFragment.Argument argument) {
        return new IAnnotationFragment(name, ImmutableList.of(argument));
    }

    @Nonnull
    private static IFunctionDefinitionFragment.Parameter parameter(@Nonnull final String name,
                                                                   @Nonnull final IType type) {
        return new IFunctionDefinitionFragment.Parameter(name, type);
    }

    @Nonnull
    private static IBlockFragment block(@Nonnull final List<IStatementFragment> statements) {
        return new IBlockFragment(statements);
    }

    /**
     * Returns the last statement of the body of the function that a program defines last.
     */
    @Nonnull
    private static IStatementFragment lastStatementOfLastFunction(@Nonnull final String text) {
        final List<IStatementFragment> statements = parseProgram(text).statements();
        final List<IStatementFragment> body =
                ((IFunctionDefinitionFragment)statements.get(statements.size() - 1)).body().statements();
        return body.get(body.size() - 1);
    }
}
