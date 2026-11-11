/*
 * IFunctionType.java
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
import java.util.stream.Collectors;

/**
 * A function type, {@code (int, string) -> bigint} or equivalently {@code function<int, string, bigint>}: the parameter
 * types followed by the result type. The result type is the one place where a type can be {@code void}.
 */
@API(API.Status.EXPERIMENTAL)
public final class IFunctionType implements IType {
    private final boolean isNullable;
    @Nonnull
    private final ImmutableList<IType> parameterTypes;
    @Nonnull
    private final IType resultType;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a function type.
     *
     * @param isNullable whether values of this type can be null
     * @param parameterTypes the types of the parameters in order, none of which can be {@code void}
     * @param resultType the type of the result
     */
    public IFunctionType(final boolean isNullable, @Nonnull final List<IType> parameterTypes,
                         @Nonnull final IType resultType) {
        this.isNullable = isNullable;
        this.parameterTypes = ImmutableList.copyOf(parameterTypes);
        this.resultType = Objects.requireNonNull(resultType);
        Preconditions.checkArgument(this.parameterTypes.stream().noneMatch(IType::isVoid),
                "void is only a result type");
    }

    @Override
    public boolean isNullable() {
        return isNullable;
    }

    /**
     * {@return the types of the parameters in order}
     */
    @Nonnull
    public List<IType> parameterTypes() {
        return parameterTypes;
    }

    /**
     * {@return the type of the result}
     */
    @Nonnull
    public IType resultType() {
        return resultType;
    }

    @Nonnull
    @Override
    public IFunctionType withNullability(final boolean isNullable) {
        return new IFunctionType(isNullable, parameterTypes, resultType);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IFunctionType that && isNullable == that.isNullable
                && parameterTypes.equals(that.parameterTypes) && resultType.equals(that.resultType);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("function type", isNullable, parameterTypes, resultType);
    }

    /**
     * Returns the arrow form, {@code (int, string) -> bigint}. A nullable function type is parenthesized before its
     * {@code ?}, {@code ((int) -> bigint)?}, because a {@code ?} directly after the result type would belong to the
     * result.
     */
    @Nonnull
    @Override
    public String toString() {
        final String arrowForm = parameterTypes.stream()
                .map(IType::toString)
                .collect(Collectors.joining(", ", "(", ")")) + " -> " + resultType;
        return isNullable ? "(" + arrowForm + ")?" : arrowForm;
    }
}
