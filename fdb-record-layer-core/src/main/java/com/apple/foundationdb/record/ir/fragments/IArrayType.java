/*
 * IArrayType.java
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

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * An array type, {@code [int]} or equivalently {@code array<int>}. Both the array and its elements can be nullable:
 * {@code [int?]?}.
 */
@API(API.Status.EXPERIMENTAL)
public final class IArrayType implements IType {
    private final boolean isNullable;
    @Nonnull
    private final IType elementType;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates an array type.
     *
     * @param isNullable whether values of this type can be null
     * @param elementType the type of the elements, which cannot be {@code void}
     */
    public IArrayType(final boolean isNullable, @Nonnull final IType elementType) {
        this.isNullable = isNullable;
        this.elementType = Objects.requireNonNull(elementType);
        Preconditions.checkArgument(!elementType.isVoid(), "void is only a result type");
    }

    @Override
    public boolean isNullable() {
        return isNullable;
    }

    /**
     * {@return the type of the elements}
     */
    @Nonnull
    public IType elementType() {
        return elementType;
    }

    @Nonnull
    @Override
    public IArrayType withNullability(final boolean isNullable) {
        return new IArrayType(isNullable, elementType);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IArrayType that && isNullable == that.isNullable && elementType.equals(that.elementType);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("array type", isNullable, elementType);
    }

    @Nonnull
    @Override
    public String toString() {
        return "[" + elementType + "]" + ParserHelpers.nullableSuffix(isNullable);
    }
}
