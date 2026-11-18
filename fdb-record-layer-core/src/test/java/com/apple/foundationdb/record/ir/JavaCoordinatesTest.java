/*
 * JavaCoordinatesTest.java
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

import com.apple.foundationdb.record.ir.fragments.IFunctionDeclarationFragment;
import com.apple.foundationdb.record.ir.fragments.IStatementFragment;
import com.apple.foundationdb.record.ir.fragments.ITypeDeclarationFragment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.ir.FrlIrParsing.parseProgram;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link JavaCoordinates}: the package of a program locates its extern declarations in Java, and the
 * {@code @java} annotation of a declaration overrides what it states.
 */
class JavaCoordinatesTest {
    private static final String PACKAGE = "@java(\"com.apple.foundationdb.record.ir\") package abc\n";

    @Test
    void coordinatesWithoutAnnotationsFollowFromPackage() {
        final List<IStatementFragment> statements = parseProgram(PACKAGE + """
                extern type range
                extern func scan(range: range) -> bigint""").statements();
        assertThat(JavaCoordinates.typeClass(typeDeclaration(statements, "range")))
                .contains("com.apple.foundationdb.record.ir.range");
        // a function is a static method of the class named after the package
        assertThat(JavaCoordinates.function(functionDeclaration(statements, "scan")))
                .contains(new JavaCoordinates.Method("com.apple.foundationdb.record.ir.abc", "scan"));
    }

    @Test
    void coordinatesAnnotationOfDeclarationOverridesPackage() {
        final List<IStatementFragment> statements = parseProgram(PACKAGE + """
                @java(class: "com.apple.foundationdb.record.TupleRange") extern type range
                @java(class: "com.apple.foundationdb.record.Scans") extern func scan(range: range) -> bigint
                @java(method: "lookUp") extern func lookup(key: bigint) -> bigint
                @java(class: "a.Keys", method: "next") extern func successor(key: bigint) -> bigint
                @swift(module: "RecordLayerIR") extern func other()""").statements();
        assertThat(JavaCoordinates.typeClass(typeDeclaration(statements, "range")))
                .contains("com.apple.foundationdb.record.TupleRange");
        // what the annotation leaves out follows from the package
        assertThat(JavaCoordinates.function(functionDeclaration(statements, "scan")))
                .contains(new JavaCoordinates.Method("com.apple.foundationdb.record.Scans", "scan"));
        assertThat(JavaCoordinates.function(functionDeclaration(statements, "lookup")))
                .contains(new JavaCoordinates.Method("com.apple.foundationdb.record.ir.abc", "lookUp"));
        assertThat(JavaCoordinates.function(functionDeclaration(statements, "successor")))
                .contains(new JavaCoordinates.Method("a.Keys", "next"));
        // annotations of other targets do not matter here
        assertThat(JavaCoordinates.function(functionDeclaration(statements, "other")))
                .contains(new JavaCoordinates.Method("com.apple.foundationdb.record.ir.abc", "other"));
    }

    @Test
    void coordinatesWithoutPackageNeedAnnotationOfDeclaration() {
        final List<IStatementFragment> statements = parseProgram("""
                extern type range
                @java(class: "a.Range") extern type interval
                extern func scan(range: range) -> bigint
                @java(class: "a.Scans") extern func scanAll()
                @java(method: "lookUp") extern func lookup(key: bigint) -> bigint""").statements();
        assertThat(JavaCoordinates.typeClass(typeDeclaration(statements, "range"))).isEmpty();
        assertThat(JavaCoordinates.typeClass(typeDeclaration(statements, "interval"))).contains("a.Range");
        assertThat(JavaCoordinates.function(functionDeclaration(statements, "scan"))).isEmpty();
        assertThat(JavaCoordinates.function(functionDeclaration(statements, "scanAll")))
                .contains(new JavaCoordinates.Method("a.Scans", "scanAll"));
        // a method needs a class
        assertThat(JavaCoordinates.function(functionDeclaration(statements, "lookup"))).isEmpty();
        // a package without a @java annotation does not locate anything in Java
        final List<IStatementFragment> swiftOnly = parseProgram("@swift(module: \"RecordLayerIR\") package abc\n"
                + "extern type range").statements();
        assertThat(JavaCoordinates.typeClass(typeDeclaration(swiftOnly, "range"))).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            PACKAGE + "@java(\"a.Range\") extern type range",                            // arguments are labeled
            PACKAGE + "@java(method: \"m\") extern type range",                          // a type has no method
            PACKAGE + "@java(class: \"a.B\", class: \"a.C\") extern type range",         // a label is used once
            PACKAGE + "@java(class: \"a.B\") @java(class: \"a.C\") extern type range",   // one @java annotation
            "@java(name: \"a\") package abc\nextern type range",                          // the package is positional
            "@java(\"a\", \"b\") package abc\nextern type range",                         // and the only argument
            "@java package abc\nextern type range",
            "@java(\"a\") @java(\"b\") package abc\nextern type range",
    })
    void typeClassMalformedAnnotationFails(@Nonnull final String text) {
        final ITypeDeclarationFragment range = typeDeclaration(parseProgram(text).statements(), "range");
        assertThatThrownBy(() -> JavaCoordinates.typeClass(range)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            PACKAGE + "@java(\"a.B\") extern func f()",
            PACKAGE + "@java(module: \"M\") extern func f()",
            PACKAGE + "@java(method: \"g\", method: \"h\") extern func f()",
            "@java(name: \"a\") package abc\nextern func f()",
    })
    void functionMalformedAnnotationFails(@Nonnull final String text) {
        final IFunctionDeclarationFragment f = functionDeclaration(parseProgram(text).statements(), "f");
        assertThatThrownBy(() -> JavaCoordinates.function(f)).isInstanceOf(IllegalArgumentException.class);
    }

    @Nonnull
    private static ITypeDeclarationFragment typeDeclaration(@Nonnull final List<IStatementFragment> statements,
                                                            @Nonnull final String name) {
        return statements.stream()
                .filter(ITypeDeclarationFragment.class::isInstance)
                .map(ITypeDeclarationFragment.class::cast)
                .filter(declaration -> declaration.name().equals(name))
                .findFirst()
                .orElseThrow();
    }

    @Nonnull
    private static IFunctionDeclarationFragment functionDeclaration(@Nonnull final List<IStatementFragment> statements,
                                                                    @Nonnull final String name) {
        return statements.stream()
                .filter(IFunctionDeclarationFragment.class::isInstance)
                .map(IFunctionDeclarationFragment.class::cast)
                .filter(declaration -> declaration.name().equals(name))
                .findFirst()
                .orElseThrow();
    }
}
