/*
 * FrlIrParsing.java
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

import com.apple.foundationdb.record.ir.fragments.IFunctionDeclaration;
import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.IProgram;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.generated.FrlIrLexer;
import com.apple.foundationdb.record.ir.generated.FrlIrParser;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Parses FRL-IR text in tests.
 */
public final class FrlIrParsing {

    private FrlIrParsing() {
    }

    /**
     * Parses a program.
     *
     * @param text the program in FRL-IR syntax
     * @return the program
     * @throws IllegalArgumentException if the text is not a program
     */
    @Nonnull
    public static IProgram parseProgram(@Nonnull final String text) {
        final FrlIrLexer lexer = new FrlIrLexer(CharStreams.fromString(text));
        lexer.removeErrorListeners();
        lexer.addErrorListener(ThrowingErrorListener.INSTANCE);
        final FrlIrParser parser = new FrlIrParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(ThrowingErrorListener.INSTANCE);
        return new FrlIrFragmentVisitor().visitProgram(parser.program());
    }

    /**
     * Parses a single type outside of any generic function.
     *
     * @param text the type in FRL-IR syntax
     * @return the type
     * @throws IllegalArgumentException if the text is not a type
     */
    @Nonnull
    public static IType parseType(@Nonnull final String text) {
        return parseType(text, ImmutableSet.of());
    }

    /**
     * Parses a single type in the scope of the given type parameters. A program is the only entry point of the grammar,
     * so the type is parsed as the result type of the function type of a parameter of a declared function with these
     * type parameters: a result type, because that is the one place where {@code void} is a type. The type stands on
     * lines of its own, so that a line comment at its end does not swallow the rest of the declaration.
     *
     * @param text the type in FRL-IR syntax
     * @param typeParameters the names of the type parameters in scope
     * @return the type
     * @throws IllegalArgumentException if the text is not a type
     */
    @Nonnull
    public static IType parseType(@Nonnull final String text, @Nonnull final Set<String> typeParameters) {
        final String typeParameterClause = typeParameters.isEmpty()
                ? ""
                : typeParameters.stream().collect(Collectors.joining(", ", "<", ">"));
        final IProgram program = parseProgram("extern func f" + typeParameterClause + "(x: () ->\n" + text + "\n)");
        final IFunctionDeclaration declaration = (IFunctionDeclaration)program.statements().get(0);
        final IFunctionType wrapper = (IFunctionType)declaration.parameters().get(0).type();
        // a `?` that the type cannot take, as in `int??` or `stream<int>?`, makes the wrapping function type nullable
        if (wrapper.isNullable()) {
            throw new IllegalArgumentException("the text is not a type");
        }
        return wrapper.resultType();
    }

    /**
     * Turns the first lexer or parser error into an exception. ANTLR's default listener prints errors and recovers, so
     * without this listener a test would pass on input that the grammar does not accept.
     */
    private static final class ThrowingErrorListener extends BaseErrorListener {
        private static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

        @Override
        public void syntaxError(final Recognizer<?, ?> recognizer, final Object offendingSymbol, final int line,
                                final int charPositionInLine, final String msg, final RecognitionException e) {
            throw new IllegalArgumentException("line " + line + ":" + charPositionInLine + " " + msg, e);
        }
    }
}
