/*
 * IFunctionDefinitionFragment.java
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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A function definition, {@code func map<T, U>(xs: [T], f: (T) -> U, ys: [U]) -> [U] { ys }}: a signature and the
 * block that implements it. The value of the block is the result of the function, so a block of a function whose
 * result type is not {@code void} ends with an expression of that type. A function definition is also a statement, so
 * functions can be nested, as in Swift; a nested function sees the type parameters of the functions it is nested in. A
 * function is in scope only after its definition, so it cannot call itself: FRL-IR has no recursion. A definition can
 * be a variant of an overloaded function, as {@code variant func add<int, int>(a: int, b: int) -> int { … }} is, see
 * {@link #variant()}.
 */
@API(API.Status.EXPERIMENTAL)
public final class IFunctionDefinitionFragment implements IFunctionFragment {
    @Nonnull
    private final String name;
    @Nonnull
    private final ImmutableList<IGenericType> typeParameters;
    @Nonnull
    private final ImmutableList<Parameter> parameters;
    @Nonnull
    private final IType resultType;
    @Nonnull
    private final IBlockFragment body;
    @Nonnull
    private final ImmutableList<IAnnotationFragment> annotations;
    @Nullable
    private final IOverloadedFunctionFragment.Variant variant;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a function definition without annotations.
     *
     * @param name the name of the function
     * @param typeParameters the type parameters of a generic function, in order, and empty otherwise; the type
     *        parameters that the signature and the body refer to are these objects
     * @param parameters the parameters in order
     * @param resultType the result type, which is {@code void} if the definition does not state one
     * @param body the block that implements the function, see {@link IBlockFragment#isBodyOf}
     */
    public IFunctionDefinitionFragment(@Nonnull final String name, @Nonnull final List<IGenericType> typeParameters,
                                       @Nonnull final List<Parameter> parameters, @Nonnull final IType resultType,
                                       @Nonnull final IBlockFragment body) {
        this(name, typeParameters, parameters, resultType, body, ImmutableList.of());
    }

    /**
     * Creates a function definition that is not a variant.
     *
     * @param name the name of the function
     * @param typeParameters the type parameters of a generic function, in order, and empty otherwise; the type
     *        parameters that the signature and the body refer to are these objects
     * @param parameters the parameters in order
     * @param resultType the result type, which is {@code void} if the definition does not state one
     * @param body the block that implements the function, see {@link IBlockFragment#isBodyOf}
     * @param annotations the annotations of the definition, in order
     */
    public IFunctionDefinitionFragment(@Nonnull final String name, @Nonnull final List<IGenericType> typeParameters,
                                       @Nonnull final List<Parameter> parameters, @Nonnull final IType resultType,
                                       @Nonnull final IBlockFragment body,
                                       @Nonnull final List<IAnnotationFragment> annotations) {
        this(name, typeParameters, parameters, resultType, body, annotations, null);
    }

    /**
     * Creates a function definition.
     *
     * @param name the name of the function
     * @param typeParameters the type parameters of a generic function, in order, and empty otherwise; the type
     *        parameters that the signature and the body refer to are these objects
     * @param parameters the parameters in order
     * @param resultType the result type, which is {@code void} if the definition does not state one
     * @param body the block that implements the function, see {@link IBlockFragment#isBodyOf}
     * @param annotations the annotations of the definition, in order
     * @param variant the overloaded function that the function is a variant of, with its key, or {@code null} if the
     *        function is not a variant; the signature must be that of the variant, see
     *        {@link IOverloadedFunctionFragment.Variant}
     */
    public IFunctionDefinitionFragment(@Nonnull final String name, @Nonnull final List<IGenericType> typeParameters,
                                       @Nonnull final List<Parameter> parameters, @Nonnull final IType resultType,
                                       @Nonnull final IBlockFragment body,
                                       @Nonnull final List<IAnnotationFragment> annotations,
                                       @Nullable final IOverloadedFunctionFragment.Variant variant) {
        this.name = Objects.requireNonNull(name);
        this.typeParameters = ImmutableList.copyOf(typeParameters);
        this.parameters = ImmutableList.copyOf(parameters);
        this.resultType = Objects.requireNonNull(resultType);
        this.body = Objects.requireNonNull(body);
        this.annotations = ImmutableList.copyOf(annotations);
        this.variant = variant;
        Preconditions.checkArgument(body.isBodyOf(resultType), "the body of a function must end with an expression of "
                + "its result type unless that is void, and it must not yield or consume");
        if (variant != null) {
            variant.checkSignature(name, this.typeParameters, this.parameters);
        }
    }

    /**
     * {@return the name of the function}
     */
    @Nonnull
    @Override
    public String name() {
        return name;
    }

    /**
     * {@return the type parameters of a generic function, in order, and an empty list otherwise}
     */
    @Nonnull
    @Override
    public List<IGenericType> typeParameters() {
        return typeParameters;
    }

    /**
     * {@return the parameters in order}
     */
    @Nonnull
    @Override
    public List<Parameter> parameters() {
        return parameters;
    }

    /**
     * {@return the result type, which is {@code void} if the definition does not state one}
     */
    @Nonnull
    @Override
    public IType resultType() {
        return resultType;
    }

    /**
     * {@return the block that implements the function}
     */
    @Nonnull
    public IBlockFragment body() {
        return body;
    }

    /**
     * {@return the annotations of the definition, in order}
     */
    @Nonnull
    @Override
    public List<IAnnotationFragment> annotations() {
        return annotations;
    }

    /**
     * {@return the overloaded function that the function is a variant of, with its key, or empty if it is none}
     */
    @Nonnull
    @Override
    public Optional<IOverloadedFunctionFragment.Variant> variant() {
        return Optional.ofNullable(variant);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IFunctionDefinitionFragment that && name.equals(that.name)
                && typeParameters.equals(that.typeParameters) && parameters.equals(that.parameters)
                && resultType.equals(that.resultType) && body.equals(that.body)
                && annotations.equals(that.annotations) && Objects.equals(variant, that.variant);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("function definition", name, typeParameters, parameters, resultType, body, annotations,
                variant);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IFunctionDefinitionFragment[name=" + name + ", typeParameters=" + typeParameters + ", parameters="
                + parameters + ", resultType=" + resultType + ", body=" + body + ", annotations=" + annotations
                + ", variant=" + variant + "]";
    }

    /**
     * A parameter of a function definition or a function declaration, {@code x: int}. Such a parameter always states
     * its type. As in Swift, a parameter has an argument label and a name. A call writes the label before the argument,
     * and the body uses the name. Examples:
     * <ul>
     *     <li>{@code x: int} has the label {@code x}, so a call writes {@code f(x: 1)};</li>
     *     <li>{@code to x: int} has the label {@code to}, so a call writes {@code f(to: 1)};</li>
     *     <li>{@code _ x: int} has no label, so a call writes {@code f(1)}.</li>
     * </ul>
     * The labels belong to the function, not to its type, so a call of a function value has no labels.
     */
    public static final class Parameter implements IFragment {
        @Nullable
        private final String label;
        @Nonnull
        private final String name;
        @Nonnull
        private final IType type;
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        /**
         * Creates a parameter whose label is its name, as {@code x: int} declares it.
         *
         * @param name the name of the parameter, which is also its label
         * @param type the type of the parameter, which cannot be {@code void}
         */
        public Parameter(@Nonnull final String name, @Nonnull final IType type) {
            this(name, name, type);
        }

        /**
         * Creates a parameter.
         *
         * @param label the argument label of the parameter, or {@code null} if the parameter has no label
         * @param name the name of the parameter
         * @param type the type of the parameter, which cannot be {@code void}
         */
        public Parameter(@Nullable final String label, @Nonnull final String name, @Nonnull final IType type) {
            this.label = label;
            this.name = Objects.requireNonNull(name);
            this.type = Objects.requireNonNull(type);
            Preconditions.checkArgument(!type.isVoid(), "void is only a result type");
        }

        /**
         * {@return the argument label of the parameter, or empty if the parameter has no label}
         */
        @Nonnull
        public Optional<String> label() {
            return Optional.ofNullable(label);
        }

        /**
         * {@return the name of the parameter}
         */
        @Nonnull
        public String name() {
            return name;
        }

        /**
         * {@return the type of the parameter}
         */
        @Nonnull
        public IType type() {
            return type;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            return other instanceof Parameter that && Objects.equals(label, that.label) && name.equals(that.name)
                    && type.equals(that.type);
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        private int computeHashCode() {
            return Objects.hash("function definition parameter", label, name, type);
        }

        @Nonnull
        @Override
        public String toString() {
            return "Parameter[label=" + label + ", name=" + name + ", type=" + type + "]";
        }
    }
}
