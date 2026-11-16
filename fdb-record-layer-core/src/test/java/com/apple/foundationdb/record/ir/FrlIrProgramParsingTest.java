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
import com.apple.foundationdb.record.ir.fragments.IAssignment;
import com.apple.foundationdb.record.ir.fragments.IBlock;
import com.apple.foundationdb.record.ir.fragments.IClosure;
import com.apple.foundationdb.record.ir.fragments.IExternType;
import com.apple.foundationdb.record.ir.fragments.IFunctionCall;
import com.apple.foundationdb.record.ir.fragments.IFunctionDeclaration;
import com.apple.foundationdb.record.ir.fragments.IFunctionDefinition;
import com.apple.foundationdb.record.ir.fragments.IFunctionReference;
import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.apple.foundationdb.record.ir.fragments.ILiteral;
import com.apple.foundationdb.record.ir.fragments.IPackageStatement;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IProgram;
import com.apple.foundationdb.record.ir.fragments.IRecordType;
import com.apple.foundationdb.record.ir.fragments.IStatement;
import com.apple.foundationdb.record.ir.fragments.IStreamFragment;
import com.apple.foundationdb.record.ir.fragments.IStreamType;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.fragments.ITypeDeclaration;
import com.apple.foundationdb.record.ir.fragments.IVariableReference;
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
 * declaration, package, annotation, and yield productions of the grammar, and the fragments that
 * {@link FrlIrFragmentVisitor} builds from them; the stream blocks that yields belong to are tested with the other
 * expressions.
 */
class FrlIrProgramParsingTest {
    private static final IType INT = new IPrimitiveType(IPrimitiveType.Kind.INT, false);
    private static final IType BIGINT = new IPrimitiveType(IPrimitiveType.Kind.BIGINT, false);
    private static final IType STRING = new IPrimitiveType(IPrimitiveType.Kind.STRING, false);
    private static final IType BOOLEAN = new IPrimitiveType(IPrimitiveType.Kind.BOOLEAN, false);
    private static final IType VOID = new IPrimitiveType(IPrimitiveType.Kind.VOID, false);
    private static final IGenericType T = new IGenericType("T", false);
    private static final IGenericType U = new IGenericType("U", false);
    private static final IBlock EMPTY_BLOCK = new IBlock(ImmutableList.of());

    @Nonnull
    static Stream<Arguments> validPrograms() {
        final IFunctionDefinition a = function("a");
        final IFunctionDefinition b = function("b");
        return Stream.of(
                // a program is a sequence of statements, separated as in a block
                Arguments.of("", new IProgram(ImmutableList.of())),
                Arguments.of("\n\n;\n", new IProgram(ImmutableList.of())),
                Arguments.of("func a() { }", new IProgram(ImmutableList.of(a))),
                Arguments.of("func a() { }; func b() { }", new IProgram(ImmutableList.of(a, b))),
                Arguments.of("\nfunc a() { }\n\nfunc b() { }\n", new IProgram(ImmutableList.of(a, b))));
    }

    @ParameterizedTest
    @MethodSource("validPrograms")
    void parseProgramValidProgramCreatesProgram(@Nonnull final String text, @Nonnull final IProgram expected) {
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
                Arguments.of("func f(x: int, y: string?): bigint { 7 }", new IFunctionDefinition("f", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("x", INT),
                                new IFunctionDefinition.Parameter("y", STRING.withNullability(true))),
                        BIGINT, new IBlock(ImmutableList.of(new ILiteral(7L, BIGINT))))),
                // a trailing semicolon does not drop the value
                Arguments.of("func f(): int { 42; }", new IFunctionDefinition("f", ImmutableList.of(), ImmutableList.of(),
                        INT, new IBlock(ImmutableList.of(new ILiteral(42, INT))))),
                // without a result type, a function returns void, and a value of its body is discarded
                Arguments.of("func g() { }",
                        new IFunctionDefinition("g", ImmutableList.of(), ImmutableList.of(), VOID, EMPTY_BLOCK)),
                Arguments.of("func g(): () { }",
                        new IFunctionDefinition("g", ImmutableList.of(), ImmutableList.of(), VOID, EMPTY_BLOCK)),
                Arguments.of("func g() { 42 }", new IFunctionDefinition("g", ImmutableList.of(), ImmutableList.of(),
                        VOID, new IBlock(ImmutableList.of(new ILiteral(42, INT))))),
                // a function type after the colon keeps its arrow, and the body's closure takes its types from it
                Arguments.of("func adder(x: int): (int) -> int { { y in x } }", new IFunctionDefinition("adder",
                        ImmutableList.of(), ImmutableList.of(new IFunctionDefinition.Parameter("x", INT)),
                        new IFunctionType(false, ImmutableList.of(INT), INT),
                        new IBlock(ImmutableList.of(new IClosure(ImmutableList.of(new IClosure.Parameter("y", INT)), INT,
                                new IBlock(ImmutableList.of(new IVariableReference("x", INT)))))))),
                // generic functions: type parameters are types within the signature
                Arguments.of("func id<T>(x: T): T { x }", new IFunctionDefinition("id", ImmutableList.of(T),
                        ImmutableList.of(new IFunctionDefinition.Parameter("x", T)), T,
                        new IBlock(ImmutableList.of(new IVariableReference("x", T))))),
                Arguments.of("func map<T, U>(xs: [T], f: (T) -> U?, ys: [U?]): [U?] { ys }", new IFunctionDefinition("map",
                        ImmutableList.of(T, U),
                        ImmutableList.of(new IFunctionDefinition.Parameter("xs", new IArrayType(false, T)),
                                new IFunctionDefinition.Parameter("f",
                                        new IFunctionType(false, ImmutableList.of(T), U.withNullability(true))),
                                new IFunctionDefinition.Parameter("ys",
                                        new IArrayType(false, U.withNullability(true)))),
                        new IArrayType(false, U.withNullability(true)),
                        new IBlock(ImmutableList.of(
                                new IVariableReference("ys", new IArrayType(false, U.withNullability(true))))))),
                // names between backticks
                Arguments.of("func `in`(`func`: int) { }", new IFunctionDefinition("in", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("func", INT)), VOID, EMPTY_BLOCK)));
    }

    @ParameterizedTest
    @MethodSource("validFunctionDefinitions")
    void parseProgramFunctionDefinitionCreatesDefinition(@Nonnull final String text,
                                                         @Nonnull final IFunctionDefinition expected) {
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
                Arguments.of("extern func f(x: int): bigint", new IFunctionDeclaration("f", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("x", INT)), BIGINT)),
                // without a result type, a declared function returns void
                Arguments.of("extern func g<T>(x: T)", new IFunctionDeclaration("g", ImmutableList.of(T),
                        ImmutableList.of(new IFunctionDefinition.Parameter("x", T)), VOID)),
                // a declaration can be split across lines wherever it cannot end
                Arguments.of("extern\nfunc\nh(\n    x: int,\n    y: int\n): (int) -> int", new IFunctionDeclaration("h",
                        ImmutableList.of(), ImmutableList.of(new IFunctionDefinition.Parameter("x", INT),
                                new IFunctionDefinition.Parameter("y", INT)),
                        new IFunctionType(false, ImmutableList.of(INT), INT))));
    }

    @ParameterizedTest
    @MethodSource("validFunctionDeclarations")
    void parseProgramFunctionDeclarationCreatesDeclaration(@Nonnull final String text,
                                                           @Nonnull final IFunctionDeclaration expected) {
        assertThat(parseProgram(text).statements()).containsExactly(expected);
    }

    @Test
    void parseProgramDeclaredFunctionCanBeCalled() {
        final IFunctionDeclaration f = new IFunctionDeclaration("f", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinition.Parameter("x", BIGINT)), STRING);
        assertThat(parseProgram("extern func f(x: bigint): string\nlet s = f(42)").statements()).containsExactly(f,
                new IAssignment("s", new IFunctionCall(new IFunctionReference(f, ImmutableList.of(), f.type()),
                        ImmutableList.of(new ILiteral(42L, BIGINT)))));
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
    static Stream<Arguments> validBlocks() {
        final IFunctionDefinition a = function("a");
        final IFunctionDefinition b = function("b");
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
                Arguments.of("func outer() { { } }",
                        function("outer", ImmutableList.of(new IClosure(ImmutableList.of(), VOID, EMPTY_BLOCK)))));
    }

    @ParameterizedTest
    @MethodSource("validBlocks")
    void parseProgramStatementsInBodyCreateStatements(@Nonnull final String text,
                                                      @Nonnull final IFunctionDefinition expected) {
        assertThat(parseProgram(text).statements()).containsExactly(expected);
    }

    @Test
    void parseProgramStatementSplitAcrossLinesIsOneStatement() {
        final IProgram program = parseProgram("""
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
                new IFunctionDefinition("c", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("x", INT),
                                new IFunctionDefinition.Parameter("y", INT)),
                        INT, new IBlock(ImmutableList.of(new IVariableReference("x", INT)))),
                new IFunctionDefinition("d", ImmutableList.of(T),
                        ImmutableList.of(new IFunctionDefinition.Parameter("xs", new IRecordType(false, ImmutableList.of(
                                IRecordType.Field.of(BIGINT, "id", 1),
                                IRecordType.Field.of(STRING.withNullability(true), "name"))))),
                        VOID, EMPTY_BLOCK));
    }

    @Test
    void parseProgramNestedFunctionSeesEnclosingTypeParameters() {
        final IProgram program = parseProgram("""
                func outer<T>(x: T) {
                    func inner<U>(y: U, z: [T]): [T] { z }
                }""");
        final IArrayType arrayOfT = new IArrayType(false, T);
        assertThat(program.statements()).containsExactly(
                new IFunctionDefinition("outer", ImmutableList.of(T),
                        ImmutableList.of(new IFunctionDefinition.Parameter("x", T)), VOID,
                        new IBlock(ImmutableList.of(new IFunctionDefinition("inner", ImmutableList.of(U),
                                ImmutableList.of(new IFunctionDefinition.Parameter("y", U),
                                        new IFunctionDefinition.Parameter("z", arrayOfT)), arrayOfT,
                                new IBlock(ImmutableList.of(new IVariableReference("z", arrayOfT))))))));
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
        final IFunctionDefinition one = new IFunctionDefinition("one", ImmutableList.of(), ImmutableList.of(), INT,
                new IBlock(ImmutableList.of(new ILiteral(1, INT))));
        return Stream.of(
                // a function returns the stream that its stream block constructs; the result type of the function is
                // the context of the stream block, and so of the values that the block yields
                Arguments.of("func numbers(): stream<bigint> {\n    stream {\n        yield 1\n"
                                + "        yield 2\n    }\n}",
                        function("numbers", new IStreamType(BIGINT), ImmutableList.of(new IStreamFragment(BIGINT,
                                new IBlock(ImmutableList.of(new IYieldFragment(new ILiteral(1L, BIGINT)),
                                        new IYieldFragment(new ILiteral(2L, BIGINT)))))))),
                // the code before the stream block runs when the function is called, and the block sees its variables
                Arguments.of("func twice<T>(x: T): stream<T> {\n    let y = x\n"
                                + "    stream {\n        yield x\n        yield y\n    }\n}",
                        new IFunctionDefinition("twice", ImmutableList.of(T),
                                ImmutableList.of(new IFunctionDefinition.Parameter("x", T)), new IStreamType(T),
                                new IBlock(ImmutableList.of(new IAssignment("y", new IVariableReference("x", T)),
                                        new IStreamFragment(T, new IBlock(ImmutableList.of(
                                                new IYieldFragment(new IVariableReference("x", T)),
                                                new IYieldFragment(new IVariableReference("y", T))))))))),
                // a yield can be split across lines after its keyword
                Arguments.of("func numbers(): stream<int> {\n    stream {\n        yield\n            1\n    }\n}",
                        function("numbers", streamOfInt, ImmutableList.of(new IStreamFragment(INT,
                                new IBlock(ImmutableList.of(new IYieldFragment(new ILiteral(1, INT)))))))),
                // a function in a stream block can compute what the block yields
                Arguments.of("func numbers(): stream<int> {\n    stream {\n        func one(): int { 1 }\n"
                                + "        yield one()\n    }\n}",
                        function("numbers", streamOfInt, ImmutableList.of(new IStreamFragment(INT, new IBlock(
                                ImmutableList.of(one, new IYieldFragment(new IFunctionCall(new IFunctionReference(one,
                                        ImmutableList.of(), one.type()), ImmutableList.of())))))))),
                // a closure can return a stream block
                Arguments.of("func numbers(): () -> stream<int> { { stream { yield 1 } } }", function("numbers",
                        new IFunctionType(false, ImmutableList.of(), streamOfInt), ImmutableList.of(new IClosure(
                                ImmutableList.of(), streamOfInt, new IBlock(ImmutableList.of(new IStreamFragment(INT,
                                        new IBlock(ImmutableList.of(new IYieldFragment(new ILiteral(1, INT))))))))))));
    }

    @ParameterizedTest
    @MethodSource("validStreamFunctions")
    void parseProgramFunctionReturnsStreamOfItsStreamBlock(@Nonnull final String text,
                                                           @Nonnull final IFunctionDefinition expected) {
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
    static Stream<Arguments> validAssignments() {
        final IAssignment bigintX = new IAssignment("x", new ILiteral(42L, BIGINT));
        return Stream.of(
                // the type of the variable is the type of its expression
                Arguments.of("let x = 42", new IAssignment("x", new ILiteral(42, INT))),
                Arguments.of("let x: bigint = 42", bigintX),
                // an assignment can be split across lines wherever it cannot end
                Arguments.of("let\nx\n:\nbigint\n=\n42", bigintX),
                // names between backticks
                Arguments.of("let `in` = 42", new IAssignment("in", new ILiteral(42, INT))));
    }

    @ParameterizedTest
    @MethodSource("validAssignments")
    void parseProgramAssignmentCreatesAssignment(@Nonnull final String text, @Nonnull final IAssignment expected) {
        assertThat(parseProgram(text).statements()).containsExactly(expected);
    }

    @Test
    void parseProgramExpressionIsStatement() {
        assertThat(parseProgram("let x = 42\nx\n-1\n{ }").statements()).containsExactly(
                new IAssignment("x", new ILiteral(42, INT)),
                new IVariableReference("x", INT),
                new ILiteral(-1, INT),
                new IClosure(ImmutableList.of(), VOID, EMPTY_BLOCK));
    }

    @Test
    void parseProgramVariableIsInScopeFromNextStatement() {
        assertThat(parseProgram("let x = 42; let y = x").statements()).containsExactly(
                new IAssignment("x", new ILiteral(42, INT)),
                new IAssignment("y", new IVariableReference("x", INT)));
    }

    @Test
    void parseProgramInnerVariableHidesOuterOne() {
        final IProgram program = parseProgram("""
                let x = 42
                func f(x: string) {
                    let y = x
                    let x = true
                    let z = x
                }
                let w = x""");
        // in f, x is first the parameter and then the variable of the body; after f, it is the outer variable again
        assertThat(program.statements()).containsExactly(
                new IAssignment("x", new ILiteral(42, INT)),
                new IFunctionDefinition("f", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("x", STRING)), VOID,
                        new IBlock(ImmutableList.of(
                                new IAssignment("y", new IVariableReference("x", STRING)),
                                new IAssignment("x", new ILiteral(true, BOOLEAN)),
                                new IAssignment("z", new IVariableReference("x", BOOLEAN))))),
                new IAssignment("w", new IVariableReference("x", INT)));
    }

    @Test
    void parseProgramReferenceRefersToDefinition() {
        final IProgram program = parseProgram("""
                func f(n: bigint): bigint {
                    let m = n
                    m
                }
                f(42)""");
        final IFunctionDefinition f = new IFunctionDefinition("f", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinition.Parameter("n", BIGINT)), BIGINT,
                new IBlock(ImmutableList.of(new IAssignment("m", new IVariableReference("n", BIGINT)),
                        new IVariableReference("m", BIGINT))));
        assertThat(program.statements()).containsExactly(f,
                // a call on its own is a statement
                new IFunctionCall(new IFunctionReference(f, ImmutableList.of(), f.type()),
                        ImmutableList.of(new ILiteral(42L, BIGINT))));
        // the name is resolved once, while parsing: the reference holds the definition of the program
        final IFunctionCall call = (IFunctionCall)program.statements().get(1);
        assertThat(((IFunctionReference)call.callee()).function()).isSameAs(program.statements().get(0));
    }

    @Test
    void parseProgramReferenceRefersToInnermostDefinition() {
        final IProgram program = parseProgram("""
                func f(): int { 1 }
                func g(): int {
                    func f(): int { 2 }
                    f()
                }""");
        final IFunctionDefinition innerF = new IFunctionDefinition("f", ImmutableList.of(), ImmutableList.of(), INT,
                new IBlock(ImmutableList.of(new ILiteral(2, INT))));
        final IFunctionDefinition g = (IFunctionDefinition)program.statements().get(1);
        assertThat(g.body().statements()).containsExactly(innerF,
                new IFunctionCall(new IFunctionReference(innerF, ImmutableList.of(), innerF.type()),
                        ImmutableList.of()));
    }

    @Test
    void parseProgramExternTypeIsTypeOfItsDeclaration() {
        final IProgram program = parseProgram("""
                extern type range
                extern func bounds(range: range, other: range?): [record<low: range, high: range?>]
                extern func merge(ranges: stream<range>, combine: (range, range) -> range): stream<range>""");
        final ITypeDeclaration rangeDeclaration = new ITypeDeclaration("range", ImmutableList.of(), null);
        final IExternType range = new IExternType(rangeDeclaration, false);
        assertThat(program.statements()).containsExactly(rangeDeclaration,
                new IFunctionDeclaration("bounds", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("range", range),
                                new IFunctionDefinition.Parameter("other", range.withNullability(true))),
                        new IArrayType(false, new IRecordType(false, ImmutableList.of(
                                IRecordType.Field.of(range, "low"),
                                IRecordType.Field.of(range.withNullability(true), "high"))))),
                new IFunctionDeclaration("merge", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("ranges", new IStreamType(range)),
                                new IFunctionDefinition.Parameter("combine",
                                        new IFunctionType(false, ImmutableList.of(range, range), range))),
                        new IStreamType(range)));
    }

    @Test
    void parseProgramValueOfExternTypePassesBetweenFunctions() {
        final IProgram program = parseProgram("""
                extern type range
                extern func everything(): range
                extern func count(r: range): bigint
                let r = everything()
                let n = count(r)""");
        final IExternType range = new IExternType(new ITypeDeclaration("range", ImmutableList.of(), null), false);
        assertThat(program.statements().get(3)).isEqualTo(new IAssignment("r", new IFunctionCall(
                new IFunctionReference((IFunctionDeclaration)program.statements().get(1), ImmutableList.of(),
                        new IFunctionType(false, ImmutableList.of(), range)), ImmutableList.of())));
        assertThat(((IAssignment)program.statements().get(4)).expression().type()).isEqualTo(BIGINT);
    }

    @Test
    void parseProgramPackageAndAnnotationsAreKeptAsWritten() {
        final IProgram program = parseProgram("""
                @java("com.apple.foundationdb.record.ir")
                @swift(module: "RecordLayerIR")
                package abc

                @java(class:
                    "com.apple.foundationdb.record.TupleRange")
                extern type range
                @java(method: "scanRecords") extern func scan(range: range): bigint
                @inline func id(x: int): int { x }""");
        final IPackageStatement abc = new IPackageStatement("abc", ImmutableList.of(
                annotation("java", new IAnnotationFragment.Argument(null, "com.apple.foundationdb.record.ir")),
                annotation("swift", new IAnnotationFragment.Argument("module", "RecordLayerIR"))));
        final ITypeDeclaration range = new ITypeDeclaration("range", ImmutableList.of(annotation("java",
                new IAnnotationFragment.Argument("class", "com.apple.foundationdb.record.TupleRange"))), abc);
        assertThat(program.packageStatement()).contains(abc);
        // the declarations of the program record its package
        assertThat(program.statements()).containsExactly(abc, range,
                new IFunctionDeclaration("scan", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("range", new IExternType(range, false))),
                        BIGINT, ImmutableList.of(annotation("java",
                                new IAnnotationFragment.Argument("method", "scanRecords"))), abc),
                new IFunctionDefinition("id", ImmutableList.of(), ImmutableList.of(
                        new IFunctionDefinition.Parameter("x", INT)), INT,
                        new IBlock(ImmutableList.of(new IVariableReference("x", INT))),
                        ImmutableList.of(new IAnnotationFragment("inline", ImmutableList.of()))));
    }

    @Test
    void parseProgramWithoutPackageHasNoPackage() {
        assertThat(parseProgram("extern type range").packageStatement()).isEmpty();
        assertThat(parseProgram("extern type range").statements())
                .containsExactly(new ITypeDeclaration("range", ImmutableList.of(), null));
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
            "extern type r\nextern type s\nextern func f(x: r)\nextern func g(): s\nf(g())",
    })
    void parseProgramInvalidExternTypeFails(@Nonnull final String text) {
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
    private static IFunctionDefinition function(@Nonnull final String name) {
        return function(name, ImmutableList.of());
    }

    /**
     * Returns a function definition without parameters that returns {@code void} and whose body holds the given
     * statements.
     */
    @Nonnull
    private static IFunctionDefinition function(@Nonnull final String name,
                                                @Nonnull final List<IStatement> statements) {
        return new IFunctionDefinition(name, ImmutableList.of(), ImmutableList.of(), VOID, new IBlock(statements));
    }

    /**
     * Returns a function definition without parameters of the given result type whose body holds the given statements.
     */
    @Nonnull
    private static IFunctionDefinition function(@Nonnull final String name, @Nonnull final IType resultType,
                                                @Nonnull final List<IStatement> statements) {
        return new IFunctionDefinition(name, ImmutableList.of(), ImmutableList.of(), resultType,
                new IBlock(statements));
    }

    @Nonnull
    private static IAnnotationFragment annotation(@Nonnull final String name,
                                                  @Nonnull final IAnnotationFragment.Argument argument) {
        return new IAnnotationFragment(name, ImmutableList.of(argument));
    }
}
