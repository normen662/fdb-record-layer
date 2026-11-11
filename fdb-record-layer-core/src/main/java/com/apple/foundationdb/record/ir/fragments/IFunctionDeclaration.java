/*
 * IFunctionDeclaration.java
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
 * A function declaration, {@code extern func lookup(key: bigint): string?}: the signature of a function that the
 * runtime provides, like an external function in MLIR. Calls and references treat a declared function like a defined
 * one; it just has no body.
 */
@API(API.Status.EXPERIMENTAL)
public final class IFunctionDeclaration implements IStatement {
    @Nonnull
    private final String name;
    @Nonnull
    private final ImmutableList<IGenericType> typeParameters;
    @Nonnull
    private final ImmutableList<IFunctionDefinition.Parameter> parameters;
    @Nonnull
    private final IType resultType;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a function declaration.
     *
     * @param name the name of the function
     * @param typeParameters the type parameters of a generic function, in order, and empty otherwise
     * @param parameters the parameters in order, with distinct names
     * @param resultType the result type, which is {@code void} if the declaration does not state one
     */
    public IFunctionDeclaration(@Nonnull final String name, @Nonnull final List<IGenericType> typeParameters,
                                @Nonnull final List<IFunctionDefinition.Parameter> parameters,
                                @Nonnull final IType resultType) {
        this.name = Objects.requireNonNull(name);
        this.typeParameters = ImmutableList.copyOf(typeParameters);
        this.parameters = ImmutableList.copyOf(parameters);
        this.resultType = Objects.requireNonNull(resultType);
        Preconditions.checkArgument(this.parameters.stream().map(IFunctionDefinition.Parameter::name).distinct().count()
                == this.parameters.size(), "the parameters of a function must have distinct names");
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
    public List<IFunctionDefinition.Parameter> parameters() {
        return parameters;
    }

    /**
     * {@return the result type, which is {@code void} if the declaration does not state one}
     */
    @Nonnull
    public IType resultType() {
        return resultType;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IFunctionDeclaration that && name.equals(that.name)
                && typeParameters.equals(that.typeParameters) && parameters.equals(that.parameters)
                && resultType.equals(that.resultType);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("function declaration", name, typeParameters, parameters, resultType);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IFunctionDeclaration[name=" + name + ", typeParameters=" + typeParameters + ", parameters="
                + parameters + ", resultType=" + resultType + "]";
    }
}
