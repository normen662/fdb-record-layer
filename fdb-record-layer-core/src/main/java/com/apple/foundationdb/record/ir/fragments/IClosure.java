/*
 * IClosure.java
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
 * A closure, {@code { (x: int, y: int) -> bigint in x }}: an expression whose value is a function, and whose type is the
 * function type of its parameter types and its result type, {@code (int, int) -> bigint}. The value of its body is its
 * result, as for a function definition. The text of a closure can leave out its types, {@code { x, y in x }}; the
 * fragment holds the types that were inferred for them. Unlike a function definition, a closure has no name and no type
 * parameters of its own.
 */
@API(API.Status.EXPERIMENTAL)
public final class IClosure implements IExpression {
    @Nonnull
    private final ImmutableList<Parameter> parameters;
    @Nonnull
    private final IType resultType;
    @Nonnull
    private final IBlock body;
    @Nonnull
    private final IFunctionType type;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a closure.
     *
     * @param parameters the parameters in order
     * @param resultType the result type
     * @param body the block that implements the closure, see {@link IBlock#isBodyOf}
     */
    public IClosure(@Nonnull final List<Parameter> parameters, @Nonnull final IType resultType,
                    @Nonnull final IBlock body) {
        this.parameters = ImmutableList.copyOf(parameters);
        this.resultType = Objects.requireNonNull(resultType);
        this.body = Objects.requireNonNull(body);
        this.type = new IFunctionType(false,
                this.parameters.stream().map(Parameter::type).collect(ImmutableList.toImmutableList()), resultType);
        Preconditions.checkArgument(body.isBodyOf(resultType),
                "the body of a closure must end with an expression of its result type unless that is void");
    }

    /**
     * {@return the parameters in order}
     */
    @Nonnull
    public List<Parameter> parameters() {
        return parameters;
    }

    /**
     * {@return the result type}
     */
    @Nonnull
    public IType resultType() {
        return resultType;
    }

    /**
     * {@return the block that implements the closure}
     */
    @Nonnull
    public IBlock body() {
        return body;
    }

    /**
     * Returns the type of the closure, the function type of its parameter types and its result type.
     *
     * @return the type of the closure
     */
    @Nonnull
    @Override
    public IFunctionType type() {
        return type;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IClosure that && parameters.equals(that.parameters)
                && resultType.equals(that.resultType) && body.equals(that.body);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("closure", parameters, resultType, body);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IClosure[parameters=" + parameters + ", resultType=" + resultType + ", body=" + body + "]";
    }

    /**
     * A parameter of a closure, {@code x: int}. The text may leave out the type, {@code x}, as in Swift; the parameter
     * then has the type that was inferred for it.
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
            return Objects.hash("closure parameter", name, type);
        }

        @Nonnull
        @Override
        public String toString() {
            return "Parameter[name=" + name + ", type=" + type + "]";
        }
    }
}
