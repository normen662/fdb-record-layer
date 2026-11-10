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

import com.apple.foundationdb.record.ir.program.IBlock;
import com.apple.foundationdb.record.ir.program.IFunctionDefinition;
import com.apple.foundationdb.record.ir.program.IProgram;
import com.apple.foundationdb.record.ir.program.IStatement;
import com.apple.foundationdb.record.ir.types.IArrayType;
import com.apple.foundationdb.record.ir.types.IFunctionType;
import com.apple.foundationdb.record.ir.types.IGenericType;
import com.apple.foundationdb.record.ir.types.IPrimitiveType;
import com.apple.foundationdb.record.ir.types.IRecordType;
import com.apple.foundationdb.record.ir.types.IType;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.ir.FrlIrParsing.parseProgram;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for parsing FRL-IR programs: the program, statement, and function definition productions of the grammar, and the
 * fragments that {@link FrlIrFragmentVisitor} builds from them.
 */
class FrlIrProgramParsingTest {
    private static final IType INT = new IPrimitiveType(IPrimitiveType.Kind.INT, false);
    private static final IType BIGINT = new IPrimitiveType(IPrimitiveType.Kind.BIGINT, false);
    private static final IType STRING = new IPrimitiveType(IPrimitiveType.Kind.STRING, false);
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
                Arguments.of("func f(x: int, y: string?): bigint { }", new IFunctionDefinition("f", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("x", INT),
                                new IFunctionDefinition.Parameter("y", STRING.withNullability(true))),
                        BIGINT, EMPTY_BLOCK)),
                // without a result type, a function returns void
                Arguments.of("func g() { }",
                        new IFunctionDefinition("g", ImmutableList.of(), ImmutableList.of(), VOID, EMPTY_BLOCK)),
                Arguments.of("func g(): () { }",
                        new IFunctionDefinition("g", ImmutableList.of(), ImmutableList.of(), VOID, EMPTY_BLOCK)),
                // a function type after the colon keeps its arrow
                Arguments.of("func adder(x: int): (int) -> int { }", new IFunctionDefinition("adder", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("x", INT)),
                        new IFunctionType(false, ImmutableList.of(INT), INT), EMPTY_BLOCK)),
                // generic functions: type parameters are types within the signature
                Arguments.of("func id<T>(x: T): T { }", new IFunctionDefinition("id", ImmutableList.of(T),
                        ImmutableList.of(new IFunctionDefinition.Parameter("x", T)), T, EMPTY_BLOCK)),
                Arguments.of("func map<T, U>(xs: [T], f: (T) -> U?): [U?] { }", new IFunctionDefinition("map",
                        ImmutableList.of(T, U),
                        ImmutableList.of(new IFunctionDefinition.Parameter("xs", new IArrayType(false, T)),
                                new IFunctionDefinition.Parameter("f",
                                        new IFunctionType(false, ImmutableList.of(T), U.withNullability(true)))),
                        new IArrayType(false, U.withNullability(true)), EMPTY_BLOCK)),
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
    })
    void parseProgramInvalidFunctionDefinitionFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Nonnull
    static Stream<Arguments> validBlocks() {
        final IFunctionDefinition a = function("a");
        final IFunctionDefinition b = function("b");
        return Stream.of(
                // statements on one line are separated by semicolons, statements on separate lines by line breaks
                Arguments.of("func outer() { func a() { }; func b() { } }", function("outer", a, b)),
                Arguments.of("func outer() {\n    func a() { }\n    func b() { }\n}", function("outer", a, b)),
                Arguments.of("func outer() {\n    func a() { }; func b() { }\n}", function("outer", a, b)),
                Arguments.of("func outer() {\r\n    func a() { }\r\n    func b() { }\r\n}", function("outer", a, b)),
                // a semicolon after a statement, blank lines, and repeated semicolons are allowed
                Arguments.of("func outer() { func a() { }; }", function("outer", a)),
                Arguments.of("func outer() {\n\n    ;; func a() { };\n\n    func b() { }\n\n}", function("outer", a, b)),
                // the line break after a line comment still separates statements
                Arguments.of("func outer() {\n    func a() { } // the first\n    func b() { }\n}",
                        function("outer", a, b)),
                // statements nest
                Arguments.of("func outer() {\n    func a() {\n        func b() { }\n    }\n}",
                        function("outer", function("a", b))));
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
                        INT, EMPTY_BLOCK),
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
                    func inner<U>(y: U): [T] { }
                }""");
        assertThat(program.statements()).containsExactly(
                new IFunctionDefinition("outer", ImmutableList.of(T),
                        ImmutableList.of(new IFunctionDefinition.Parameter("x", T)), VOID,
                        new IBlock(ImmutableList.of(new IFunctionDefinition("inner", ImmutableList.of(U),
                                ImmutableList.of(new IFunctionDefinition.Parameter("y", U)), new IArrayType(false, T),
                                EMPTY_BLOCK)))));
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

    /**
     * Returns a function definition without parameters that returns {@code void} and whose body holds the given
     * statements.
     */
    @Nonnull
    private static IFunctionDefinition function(@Nonnull final String name, @Nonnull final IStatement... statements) {
        return new IFunctionDefinition(name, ImmutableList.of(), ImmutableList.of(), VOID,
                new IBlock(ImmutableList.copyOf(statements)));
    }
}
