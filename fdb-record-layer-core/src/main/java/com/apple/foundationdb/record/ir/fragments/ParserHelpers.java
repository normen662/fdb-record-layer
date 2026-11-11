/*
 * ParserHelpers.java
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

package com.apple.foundationdb.record.ir.fragments;

import com.apple.foundationdb.annotation.API;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

/**
 * Helpers for the spelling of FRL-IR text that both reading and writing it need. Keeping both directions here keeps
 * them consistent, so that text written from a type reads back as an equal type. The helpers work on strings only, so
 * the fragments can use them without depending on the parser.
 *
 * <p>As in Swift, a name, such as the name of a record field or an enum value, is written as is if it is an identifier
 * that is not a keyword, and between backticks otherwise. The keywords here must be those of {@code FrlIrLexer.g4}: a
 * missing keyword would be written without backticks, and the text would not parse. A test checks every keyword of the
 * lexer for that reason.</p>
 */
@API(API.Status.INTERNAL)
public final class ParserHelpers {

    @Nonnull
    private static final ImmutableSet<String> KEYWORDS = ImmutableSet.of(
            "boolean", "int", "bigint", "half", "float", "double", "string", "bytes", "uuid", "version", "void",
            "record", "array", "stream", "vector", "enum", "function", "func", "extern", "in", "let", "true", "false",
            "nil");

    @Nonnull
    private static final Pattern IDENTIFIER = Pattern.compile("[a-zA-Z_][a-zA-Z_0-9]*");

    private ParserHelpers() {
    }

    /**
     * Checks that a name can be written. Backticks can enclose any text except backticks and line breaks, so a name
     * that is empty or contains one of them has no FRL-IR spelling.
     *
     * @param name the name
     */
    public static void checkWritableName(@Nonnull final String name) {
        Preconditions.checkArgument(!name.isEmpty() &&
                        name.chars().noneMatch(c -> c == '`' || c == '\r' || c == '\n'),
                "a name must not be empty or contain a backtick or a line break");
    }

    /**
     * Returns a name in FRL-IR syntax.
     *
     * @param name the name
     * @return the name itself if it is an identifier that is not a keyword, and the name between backticks otherwise
     */
    @Nonnull
    public static String writeName(@Nonnull final String name) {
        return IDENTIFIER.matcher(name).matches() && !KEYWORDS.contains(name) ? name : "`" + name + "`";
    }

    /**
     * Returns the name that the text of an identifier denotes, the inverse of {@link #writeName}: the text itself, or
     * the text between the backticks if the identifier is escaped. The lexer guarantees that an escaped identifier
     * starts and ends with a backtick, and that a plain identifier contains none.
     *
     * @param identifierText the text of an identifier as it appears in FRL-IR text
     * @return the name
     */
    @Nonnull
    public static String readName(@Nonnull final String identifierText) {
        return identifierText.startsWith("`")
                ? identifierText.substring(1, identifierText.length() - 1)
                : identifierText;
    }

    /**
     * Returns the suffix that marks a type as nullable, as in {@code int?}.
     *
     * @param isNullable whether the type is nullable
     * @return {@code ?} if the type is nullable, and the empty string otherwise
     */
    @Nonnull
    public static String nullableSuffix(final boolean isNullable) {
        return isNullable ? "?" : "";
    }
}
