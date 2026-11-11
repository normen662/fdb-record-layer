/*
 * IFunctionDefinition.java
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
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A function definition, {@code func map<T, U>(xs: [T], f: (T) -> U, ys: [U]): [U] { ys }}: a signature and the block
 * that implements it. The value of the block is the result of the function, so a block of a function whose result type
 * is not {@code void} ends with an expression of that type. A function definition is also a statement, so functions can
 * be nested, as in Swift; a nested function sees the type parameters of the functions it is nested in.
 */
@API(API.Status.EXPERIMENTAL)
public final class IFunctionDefinition implements IStatement {
    @Nonnull
    private final String name;
    @Nonnull
    private final ImmutableList<IGenericType> typeParameters;
    @Nonnull
    private final ImmutableList<Parameter> parameters;
    @Nonnull
    private final IType resultType;
    @Nonnull
    private final IBlock body;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a function definition.
     *
     * @param name the name of the function
     * @param typeParameters the type parameters of a generic function, in order, and empty otherwise; the type
     *        parameters that the signature and the body refer to are these objects
     * @param parameters the parameters in order
     * @param resultType the result type, which is {@code void} if the definition does not state one
     * @param body the block that implements the function, see {@link IBlock#isBodyOf}
     */
    public IFunctionDefinition(@Nonnull final String name, @Nonnull final List<IGenericType> typeParameters,
                               @Nonnull final List<Parameter> parameters, @Nonnull final IType resultType,
                               @Nonnull final IBlock body) {
        this.name = Objects.requireNonNull(name);
        this.typeParameters = ImmutableList.copyOf(typeParameters);
        this.parameters = ImmutableList.copyOf(parameters);
        this.resultType = Objects.requireNonNull(resultType);
        this.body = Objects.requireNonNull(body);
        Preconditions.checkArgument(body.isBodyOf(resultType),
                "the body of a function must end with an expression of its result type unless that is void");
    }

    /**
     * {@return the name of the function}
     */
    @Nonnull
    public String name() {
        return name;
    }

    /**
     * {@return the type parameters of a generic function, in order, and an empty list otherwise}
     */
    @Nonnull
    public List<IGenericType> typeParameters() {
        return typeParameters;
    }

    /**
     * {@return the parameters in order}
     */
    @Nonnull
    public List<Parameter> parameters() {
        return parameters;
    }

    /**
     * {@return the result type, which is {@code void} if the definition does not state one}
     */
    @Nonnull
    public IType resultType() {
        return resultType;
    }

    /**
     * {@return the block that implements the function}
     */
    @Nonnull
    public IBlock body() {
        return body;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IFunctionDefinition that && name.equals(that.name)
                && typeParameters.equals(that.typeParameters) && parameters.equals(that.parameters)
                && resultType.equals(that.resultType) && body.equals(that.body);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("function definition", name, typeParameters, parameters, resultType, body);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IFunctionDefinition[name=" + name + ", typeParameters=" + typeParameters + ", parameters="
                + parameters + ", resultType=" + resultType + ", body=" + body + "]";
    }

    /**
     * A parameter of a function definition or a function declaration, {@code x: int}. Such a parameter always states
     * its type.
     */
    public static final class Parameter implements IFragment {
        @Nonnull
        private final String name;
        @Nonnull
        private final IType type;
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        /**
         * Creates a parameter.
         *
         * @param name the name of the parameter
         * @param type the type of the parameter, which cannot be {@code void}
         */
        public Parameter(@Nonnull final String name, @Nonnull final IType type) {
            this.name = Objects.requireNonNull(name);
            this.type = Objects.requireNonNull(type);
            Preconditions.checkArgument(!type.isVoid(), "void is only a result type");
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
            return other instanceof Parameter that && name.equals(that.name) && type.equals(that.type);
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        private int computeHashCode() {
            return Objects.hash("function definition parameter", name, type);
        }

        @Nonnull
        @Override
        public String toString() {
            return "Parameter[name=" + name + ", type=" + type + "]";
        }
    }
}
