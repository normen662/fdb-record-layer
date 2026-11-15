/*
 * JavaCoordinates.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ir.fragments.IAnnotationFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionDeclaration;
import com.apple.foundationdb.record.ir.fragments.IPackageStatement;
import com.apple.foundationdb.record.ir.fragments.ITypeDeclaration;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Where the extern declarations of an FRL-IR program live in Java: the class of an extern type, and the class and the
 * static method of an extern function. The {@code @java} annotation of a package says which Java package the FRL-IR
 * package corresponds to, {@code @java("com.apple.foundationdb.record.ir") package abc}, and that locates every extern
 * declaration of the program:
 * <ul>
 *     <li>an extern type {@code range} is the class {@code com.apple.foundationdb.record.ir.range};</li>
 *     <li>an extern function {@code scan} is the static method {@code scan} of the class
 *     {@code com.apple.foundationdb.record.ir.abc}, the class named after the FRL-IR package.</li>
 * </ul>
 * A declaration needs an annotation of its own only where it lives elsewhere. Its {@code @java} annotation can state the
 * class, {@code @java(class: "com.apple.foundationdb.record.TupleRange")}, and for a function also the method,
 * {@code @java(method: "scanRecords")}; what it leaves out follows from the package.
 *
 * <p>FRL-IR keeps annotations as written, so this is where {@code @java} annotations are checked: an annotation that
 * says something this class does not understand is rejected rather than ignored.</p>
 */
@API(API.Status.EXPERIMENTAL)
public final class JavaCoordinates {
    @Nonnull
    private static final String ANNOTATION = "java";
    @Nonnull
    private static final String CLASS = "class";
    @Nonnull
    private static final String METHOD = "method";

    private JavaCoordinates() {
    }

    /**
     * Returns the Java class of an extern type.
     *
     * @param declaration the declaration of the type
     * @return the fully qualified name of the class, or empty if neither the declaration nor its package says where
     *         the type lives in Java
     * @throws IllegalArgumentException if a {@code @java} annotation of the declaration or its package is malformed
     */
    @Nonnull
    public static Optional<String> typeClass(@Nonnull final ITypeDeclaration declaration) {
        return declarationAnnotation(declaration.annotations(), ImmutableSet.of(CLASS))
                .flatMap(annotation -> annotation.argument(CLASS))
                .or(() -> javaPackage(declaration.packageStatement())
                        .map(javaPackage -> javaPackage + "." + declaration.name()));
    }

    /**
     * Returns the Java method of an extern function.
     *
     * @param declaration the declaration of the function
     * @return the class and the static method, or empty if neither the declaration nor its package says which class the
     *         method belongs to
     * @throws IllegalArgumentException if a {@code @java} annotation of the declaration or its package is malformed
     */
    @Nonnull
    public static Optional<Method> function(@Nonnull final IFunctionDeclaration declaration) {
        final Optional<IAnnotationFragment> annotation =
                declarationAnnotation(declaration.annotations(), ImmutableSet.of(CLASS, METHOD));
        final String methodName = annotation.flatMap(javaAnnotation -> javaAnnotation.argument(METHOD))
                .orElse(declaration.name());
        return annotation.flatMap(javaAnnotation -> javaAnnotation.argument(CLASS))
                .or(() -> packageClass(declaration.packageStatement()))
                .map(className -> new Method(className, methodName));
    }

    /**
     * Returns the class that holds the extern functions of a package, which is named after the FRL-IR package.
     */
    @Nonnull
    private static Optional<String> packageClass(@Nullable final IPackageStatement packageStatement) {
        return javaPackage(packageStatement)
                .map(javaPackage -> javaPackage + "." + Objects.requireNonNull(packageStatement).name());
    }

    /**
     * Returns the {@code @java} annotation of a declaration, which may only have the given labeled arguments, each at
     * most once.
     */
    @Nonnull
    private static Optional<IAnnotationFragment> declarationAnnotation(
            @Nonnull final List<IAnnotationFragment> annotations, @Nonnull final ImmutableSet<String> labels) {
        final Optional<IAnnotationFragment> annotation = javaAnnotation(annotations);
        annotation.ifPresent(javaAnnotation -> {
            final List<IAnnotationFragment.Argument> arguments = javaAnnotation.arguments();
            final boolean hasKnownLabels = arguments.stream()
                    .allMatch(argument -> argument.label() != null && labels.contains(argument.label()));
            Preconditions.checkArgument(hasKnownLabels
                            && arguments.stream().map(IAnnotationFragment.Argument::label).distinct().count()
                            == arguments.size(),
                    "the @java annotation of a declaration has labeled arguments, each once: class, and method for a "
                            + "function");
        });
        return annotation;
    }

    /**
     * Returns the Java package that the {@code @java} annotation of a package names: its only argument, without a label.
     */
    @Nonnull
    private static Optional<String> javaPackage(@Nullable final IPackageStatement packageStatement) {
        if (packageStatement == null) {
            return Optional.empty();
        }
        return javaAnnotation(packageStatement.annotations()).map(annotation -> {
            final List<IAnnotationFragment.Argument> arguments = annotation.arguments();
            Preconditions.checkArgument(arguments.size() == 1 && arguments.get(0).label() == null,
                    "the @java annotation of a package has the Java package as its only argument, without a label");
            return arguments.get(0).value();
        });
    }

    @Nonnull
    private static Optional<IAnnotationFragment> javaAnnotation(@Nonnull final List<IAnnotationFragment> annotations) {
        final ImmutableList<IAnnotationFragment> javaAnnotations = annotations.stream()
                .filter(annotation -> ANNOTATION.equals(annotation.name()))
                .collect(ImmutableList.toImmutableList());
        Preconditions.checkArgument(javaAnnotations.size() <= 1,
                "an extern declaration or a package has one @java annotation at most");
        return javaAnnotations.stream().findFirst();
    }

    /**
     * The Java coordinates of an extern function: a static method of a class.
     *
     * @param className the fully qualified name of the class
     * @param methodName the name of the method
     */
    public record Method(@Nonnull String className, @Nonnull String methodName) {
    }
}
