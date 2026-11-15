/*
 * IFunctionReference.java
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
 * A reference to a function, as the callee of a call, {@code f} in {@code f(1)}, or as a value of function type,
 * {@code f} in {@code let g = f}. The name of a function is resolved once, while parsing, and the reference refers to
 * the {@link IFunction} that the name stood for there: the definition that lowering compiles, or the declaration that
 * says what to call. A reference to a generic function fixes its type arguments, which are inferred from where the
 * reference is used: in {@code id(42)} for {@code func id<T>(x: T): T}, the reference is to {@code id} with the type
 * argument {@code int}, and its type is {@code (int) -> int}.
 */
@API(API.Status.EXPERIMENTAL)
public final class IFunctionReference implements IExpression {
    @Nonnull
    private final IFunction function;
    @Nonnull
    private final ImmutableList<IType> typeArguments;
    @Nonnull
    private final IFunctionType type;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a reference to a function.
     *
     * @param function the function that the reference refers to
     * @param typeArguments the types for the type parameters of the function, in order, and empty if the function is not
     *        generic
     * @param type the type of the function for these type arguments
     */
    public IFunctionReference(@Nonnull final IFunction function, @Nonnull final List<IType> typeArguments,
                              @Nonnull final IFunctionType type) {
        this.function = Objects.requireNonNull(function);
        this.typeArguments = ImmutableList.copyOf(typeArguments);
        this.type = Objects.requireNonNull(type);
        Preconditions.checkArgument(this.typeArguments.size() == function.typeParameters().size(),
                "a reference to a function has a type argument for each type parameter of the function");
    }

    /**
     * {@return the function that the reference refers to}
     */
    @Nonnull
    public IFunction function() {
        return function;
    }

    /**
     * {@return the name of the function}
     */
    @Nonnull
    public String name() {
        return function.name();
    }

    /**
     * {@return the types for the type parameters of the function, in order}
     */
    @Nonnull
    public List<IType> typeArguments() {
        return typeArguments;
    }

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
        return other instanceof IFunctionReference that && function.equals(that.function)
                && typeArguments.equals(that.typeArguments) && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("function reference", function, typeArguments, type);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IFunctionReference[name=" + name() + ", typeArguments=" + typeArguments + ", type=" + type + "]";
    }
}
