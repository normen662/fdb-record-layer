/*
 * FrlIrTypeParsingTest.java
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

import com.apple.foundationdb.record.ir.generated.FrlIrLexer;
import com.apple.foundationdb.record.ir.types.IArrayType;
import com.apple.foundationdb.record.ir.types.IEnumType;
import com.apple.foundationdb.record.ir.types.IFunctionType;
import com.apple.foundationdb.record.ir.types.IPrimitiveType;
import com.apple.foundationdb.record.ir.types.IRecordType;
import com.apple.foundationdb.record.ir.types.IStreamType;
import com.apple.foundationdb.record.ir.types.IType;
import com.apple.foundationdb.record.ir.types.IVectorType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.ir.FrlIrParsing.parseType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for parsing FRL-IR types: the type productions of the grammar, and the {@link IType}s that
 * {@link FrlIrFragmentVisitor} builds from them. The inputs double as a reference for how FRL-IR types are written.
 */
class FrlIrTypeParsingTest {
    private static final IType INT = primitive(IPrimitiveType.Kind.INT);
    private static final IType BIGINT = primitive(IPrimitiveType.Kind.BIGINT);
    private static final IType STRING = primitive(IPrimitiveType.Kind.STRING);

    @Nonnull
    static Stream<Arguments> validTypes() {
        return Stream.of(
                // primitive types, plain and nullable
                Arguments.of("boolean", primitive(IPrimitiveType.Kind.BOOLEAN)),
                Arguments.of("int", INT),
                Arguments.of("bigint", BIGINT),
                Arguments.of("half", primitive(IPrimitiveType.Kind.HALF)),
                Arguments.of("float", primitive(IPrimitiveType.Kind.FLOAT)),
                Arguments.of("double", primitive(IPrimitiveType.Kind.DOUBLE)),
                Arguments.of("string", STRING),
                Arguments.of("bytes", primitive(IPrimitiveType.Kind.BYTES)),
                Arguments.of("uuid", primitive(IPrimitiveType.Kind.UUID)),
                Arguments.of("version", primitive(IPrimitiveType.Kind.VERSION)),
                Arguments.of("void", primitive(IPrimitiveType.Kind.VOID)),
                Arguments.of("int?", INT.withNullability(true)),
                // records: variadic, with optional field names and field numbers
                Arguments.of("record<int, string?>?", new IRecordType(true, ImmutableList.of(
                        IRecordType.Field.unnamedOf(INT),
                        IRecordType.Field.unnamedOf(STRING.withNullability(true))))),
                Arguments.of("record<ID(1): bigint, FNAME(2): string?, LNAME: string?>", new IRecordType(false,
                        ImmutableList.of(
                                IRecordType.Field.of(BIGINT, "ID", 1),
                                IRecordType.Field.of(STRING.withNullability(true), "FNAME", 2),
                                IRecordType.Field.of(STRING.withNullability(true), "LNAME")))),
                Arguments.of("record<>", new IRecordType(false, ImmutableList.of())),
                // names between backticks can be spelled like keywords or contain other characters
                Arguments.of("record<`version`(1): bigint, `a.b`: int, `first name`: string>", new IRecordType(false,
                        ImmutableList.of(
                                IRecordType.Field.of(BIGINT, "version", 1),
                                IRecordType.Field.of(INT, "a.b"),
                                IRecordType.Field.of(STRING, "first name")))),
                Arguments.of("enum<`light blue` = 0, `enum` = 1>", new IEnumType(false, ImmutableList.of(
                        new IEnumType.Value("light blue", 0),
                        new IEnumType.Value("enum", 1)))),
                // arrays, generic and shorthand
                Arguments.of("array<int>", new IArrayType(false, INT)),
                Arguments.of("[int?]?", new IArrayType(true, INT.withNullability(true))),
                // vectors: the element type determines the precision
                Arguments.of("vector<half, 10>", new IVectorType(false, IPrimitiveType.Kind.HALF, 10)),
                Arguments.of("vector<float, 3>?", new IVectorType(true, IPrimitiveType.Kind.FLOAT, 3)),
                Arguments.of("vector<double, 512>", new IVectorType(false, IPrimitiveType.Kind.DOUBLE, 512)),
                // enums
                Arguments.of("enum<RED = 0, GREEN = 1, BLUE = -1>?", new IEnumType(true, ImmutableList.of(
                        new IEnumType.Value("RED", 0),
                        new IEnumType.Value("GREEN", 1),
                        new IEnumType.Value("BLUE", -1)))),
                // functions, generic and arrow form; a `?` after the result type belongs to the result
                Arguments.of("function<int, string, bigint>",
                        new IFunctionType(false, ImmutableList.of(INT, STRING), BIGINT)),
                Arguments.of("(int) -> bigint?",
                        new IFunctionType(false, ImmutableList.of(INT), BIGINT.withNullability(true))),
                Arguments.of("((int) -> bigint)?", new IFunctionType(true, ImmutableList.of(INT), BIGINT)),
                Arguments.of("() -> stream<int>", new IFunctionType(false, ImmutableList.of(), new IStreamType(INT))),
                Arguments.of("(int) -> (bigint) -> string", new IFunctionType(false, ImmutableList.of(INT),
                        new IFunctionType(false, ImmutableList.of(BIGINT), STRING))),
                // streams are never nullable, but their elements can be
                Arguments.of("stream<record<ID: bigint>?>", new IStreamType(new IRecordType(true,
                        ImmutableList.of(IRecordType.Field.of(BIGINT, "ID"))))),
                // line breaks are whitespace, line comments end at the line break, and block comments nest
                Arguments.of("record< // the key comes first\n  ID(1): bigint,\n  /* the /* nested */ name */ NAME: string\n>",
                        new IRecordType(false, ImmutableList.of(
                                IRecordType.Field.of(BIGINT, "ID", 1),
                                IRecordType.Field.of(STRING, "NAME")))),
                // a type may be split across lines, and an arrow may begin the next line
                Arguments.of("\n[\n    int\n]\n", new IArrayType(false, INT)),
                Arguments.of("(int)\n    -> bigint", new IFunctionType(false, ImmutableList.of(INT), BIGINT)));
    }

    @ParameterizedTest
    @MethodSource("validTypes")
    void parseTypeValidTypeCreatesType(@Nonnull final String text, @Nonnull final IType expectedType) {
        assertThat(parseType(text)).isEqualTo(expectedType);
    }

    @ParameterizedTest
    @MethodSource("validTypes")
    void toStringParsedTypeParsesToEqualType(@Nonnull final String text, @Nonnull final IType expectedType) {
        assertThat(parseType(expectedType.toString())).isEqualTo(expectedType);
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "array<int>                           | [int]",
            "array<array<string?>>?               | [[string?]]?",
            "function<int, string?, bigint?>      | (int, string?) -> bigint?",
            "function<int>                        | () -> int",
            "function<int, bigint>?               | ((int) -> bigint)?",
            "function<int, function<bigint, int>> | (int) -> (bigint) -> int",
            "function<function<int, int>, int>    | ((int) -> int) -> int",
            "(int)                                | int",
            "((int?))                             | int?",
            "()                                   | void",
            "() -> ()                             | () -> void",
            "function<()>                         | () -> void",
            "record<`ID`(1): bigint>              | record<ID(1): bigint>",
            "enum<`RED` = 0>                      | enum<RED = 0>",
    })
    void parseTypeEquivalentSpellingsCreateEqualTypes(@Nonnull final String text, @Nonnull final String canonical) {
        assertThat(parseType(text)).isEqualTo(parseType(canonical));
        assertThat(parseType(text)).hasToString(canonical);
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "T                       | T",
            "[T?]?                   | [T?]?",
            "function<T, U>          | (T) -> U",
            "record<x: T, `y`: U?>   | record<x: T, y: U?>",
    })
    void parseTypeTypeParameterInScopeCreatesGenericType(@Nonnull final String text,
                                                         @Nonnull final String canonical) {
        final ImmutableSet<String> typeParameters = ImmutableSet.of("T", "U");
        final IType type = parseType(text, typeParameters);
        assertThat(type).hasToString(canonical);
        assertThat(parseType(type.toString(), typeParameters)).isEqualTo(type);
    }

    @ParameterizedTest
    @ValueSource(strings = {"T", "[T]", "(T) -> int", "record<x: U>"})
    void parseTypeTypeParameterNotInScopeFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseType(text, ImmutableSet.of("V"))).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Returns the keywords of the lexer, i.e. its tokens that are spelled with letters only.
     */
    @Nonnull
    static Stream<String> keywords() {
        return IntStream.rangeClosed(1, FrlIrLexer.VOCABULARY.getMaxTokenType())
                .mapToObj(FrlIrLexer.VOCABULARY::getLiteralName)
                .filter(Objects::nonNull)
                .map(literalName -> literalName.substring(1, literalName.length() - 1))
                .filter(literal -> literal.chars().allMatch(Character::isLetter));
    }

    @ParameterizedTest
    @MethodSource("keywords")
    void toStringNameSpelledLikeKeywordIsEscaped(@Nonnull final String keyword) {
        final IType type = new IRecordType(false, ImmutableList.of(IRecordType.Field.of(INT, keyword, 1)));
        assertThat(type).hasToString("record<`" + keyword + "`(1): int>");
        assertThat(parseType(type.toString())).isEqualTo(type);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "a`b", "a\nb", "a\rb"})
    void fieldNameWithoutSpellingFails(@Nonnull final String name) {
        assertThatThrownBy(() -> IRecordType.Field.of(INT, name)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IEnumType.Value(name, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            // type names are lowercase keywords, and there are no placeholder types
            "Int", "INT", "long", "integer", "unknown", "null", "none", "any",
            // nullability applies at most once, and never to a stream
            "int??", "(int?)?", "stream<int>?", "(stream<int>)?",
            // the `?` of a nullable type stays on the line of its type
            "int\n?", "record<x: int\n?>",
            // keywords are hard keywords, so they cannot name a field unless they are between backticks
            "record<version: int>", "record<int: int>",
            // a name between backticks is not empty, and it ends before the end of the line
            "record<``: int>", "record<`a: int>", "record<`a\nb`: int>",
            // malformed records: a field number needs a name and is a plain number
            "record<id bigint>", "record<int,>", "record<id:>", "record<(1): int>", "record<id(x): int>",
            "record<id(-1): int>", "record<id(): int>",
            // malformed arrays
            "array<>", "array<int, int>", "[]", "[int, int]",
            // malformed vectors
            "vector<int, 3>", "vector<float>", "vector<3, float>", "vector<float, -3>",
            // malformed enums
            "enum<>", "enum<RED>", "enum<RED = x>", "enum E<RED = 0>",
            // malformed functions
            "function<>", "(int) ->", "(int,) -> int", "int -> int",
            // a block comment must be closed
            "/* not closed int",
    })
    void parseTypeInvalidTypeFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseType(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Nonnull
    private static IType primitive(@Nonnull final IPrimitiveType.Kind kind) {
        return new IPrimitiveType(kind, false);
    }
}
