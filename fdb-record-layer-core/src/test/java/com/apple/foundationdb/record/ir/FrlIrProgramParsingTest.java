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

import com.apple.foundationdb.record.ir.fragments.IArrayType;
import com.apple.foundationdb.record.ir.fragments.IAssignment;
import com.apple.foundationdb.record.ir.fragments.IBlock;
import com.apple.foundationdb.record.ir.fragments.IClosure;
import com.apple.foundationdb.record.ir.fragments.IFunctionCall;
import com.apple.foundationdb.record.ir.fragments.IFunctionDeclaration;
import com.apple.foundationdb.record.ir.fragments.IFunctionDefinition;
import com.apple.foundationdb.record.ir.fragments.IFunctionReference;
import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.apple.foundationdb.record.ir.fragments.ILiteral;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IProgram;
import com.apple.foundationdb.record.ir.fragments.IRecordType;
import com.apple.foundationdb.record.ir.fragments.IStatement;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.fragments.IVariableReference;
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
 * Tests for parsing FRL-IR programs: the program, statement, function definition, and function declaration productions
 * of the grammar, and the fragments that {@link FrlIrFragmentVisitor} builds from them.
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
        assertThat(parseProgram("extern func f(x: bigint): string\nlet s = f(42)").statements()).containsExactly(
                new IFunctionDeclaration("f", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("x", BIGINT)), STRING),
                new IAssignment("s", new IFunctionCall(new IFunctionReference("f", ImmutableList.of(),
                        new IFunctionType(false, ImmutableList.of(BIGINT), STRING)),
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
    void parseProgramFunctionBodyCallsFunctionItself() {
        final IProgram program = parseProgram("""
                func f(n: bigint): bigint {
                    let m = f(n)
                    m
                }
                f(42)""");
        final IFunctionReference f = new IFunctionReference("f", ImmutableList.of(),
                new IFunctionType(false, ImmutableList.of(BIGINT), BIGINT));
        assertThat(program.statements()).containsExactly(
                new IFunctionDefinition("f", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("n", BIGINT)), BIGINT,
                        new IBlock(ImmutableList.of(new IAssignment("m", new IFunctionCall(f,
                                ImmutableList.of(new IVariableReference("n", BIGINT)))),
                                new IVariableReference("m", BIGINT)))),
                // a call on its own is a statement
                new IFunctionCall(f, ImmutableList.of(new ILiteral(42L, BIGINT))));
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
}
