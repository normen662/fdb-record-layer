/*
 * IAnnotationFragment.java
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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * An annotation, {@code @java(class: "com.apple.foundationdb.record.TupleRange")}: a name and string arguments, each of
 * which may be labeled. Annotations say how a package, a function, or an extern type lives outside FRL-IR. FRL-IR keeps
 * them as written, and what an annotation means is up to the target that binds the annotated declaration, such as the
 * Java runtime for {@code @java}. The one exception is {@code @operator("+")}, which the parser reads: it makes the
 * annotated function the function that the operator calls. It stays among the annotations of the function.
 */
@API(API.Status.EXPERIMENTAL)
public final class IAnnotationFragment implements IFragment {
    @Nonnull
    private final String name;
    @Nonnull
    private final ImmutableList<Argument> arguments;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates an annotation.
     *
     * @param name the name of the annotation, without the {@code @}
     * @param arguments the arguments in order
     */
    public IAnnotationFragment(@Nonnull final String name, @Nonnull final List<Argument> arguments) {
        this.name = Objects.requireNonNull(name);
        this.arguments = ImmutableList.copyOf(arguments);
        ParserHelpers.checkWritableName(name);
    }

    /**
     * {@return the name of the annotation, without the {@code @}}
     */
    @Nonnull
    public String name() {
        return name;
    }

    /**
     * {@return the arguments in order}
     */
    @Nonnull
    public List<Argument> arguments() {
        return arguments;
    }

    /**
     * Returns the value of the first argument with the given label.
     *
     * @param label the label of the argument
     * @return the value, or empty if no argument has the label
     */
    @Nonnull
    public Optional<String> argument(@Nonnull final String label) {
        return arguments.stream()
                .filter(argument -> label.equals(argument.label()))
                .map(Argument::value)
                .findFirst();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IAnnotationFragment that && name.equals(that.name) && arguments.equals(that.arguments);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("annotation", name, arguments);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IAnnotationFragment[name=" + name + ", arguments=" + arguments + "]";
    }

    /**
     * An argument of an annotation, a string that may be labeled: {@code "com.apple.foundationdb.record.ir"} or
     * {@code class: "com.apple.foundationdb.record.TupleRange"}.
     */
    public static final class Argument implements IFragment {
        @Nullable
        private final String label;
        @Nonnull
        private final String value;
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        /**
         * Creates an argument.
         *
         * @param label the label of the argument, or {@code null} if the argument has none
         * @param value the value of the argument
         */
        public Argument(@Nullable final String label, @Nonnull final String value) {
            this.label = label;
            this.value = Objects.requireNonNull(value);
            if (label != null) {
                ParserHelpers.checkWritableName(label);
            }
        }

        /**
         * {@return the label of the argument, or {@code null} if the argument has none}
         */
        @Nullable
        public String label() {
            return label;
        }

        /**
         * {@return the value of the argument}
         */
        @Nonnull
        public String value() {
            return value;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            return other instanceof Argument that && Objects.equals(label, that.label) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        private int computeHashCode() {
            return Objects.hash("annotation argument", label, value);
        }

        @Nonnull
        @Override
        public String toString() {
            return "Argument[label=" + label + ", value=" + value + "]";
        }
    }
}
