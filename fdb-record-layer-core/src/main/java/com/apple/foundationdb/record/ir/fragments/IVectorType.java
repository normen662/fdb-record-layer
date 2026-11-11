/*
 * IVectorType.java
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
 * A vector type of fixed dimension, {@code vector<float, 768>}. The element type, one of {@code half}, {@code float},
 * and {@code double}, determines the precision of the vector.
 */
@API(API.Status.EXPERIMENTAL)
public final class IVectorType implements IType {
    private final boolean isNullable;
    @Nonnull
    private final IPrimitiveType.Kind elementKind;
    private final int dimensions;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a vector type.
     *
     * @param isNullable whether values of this type can be null
     * @param elementKind the type of the elements, one of {@code half}, {@code float}, and {@code double}
     * @param dimensions the number of elements
     */
    public IVectorType(final boolean isNullable, @Nonnull final IPrimitiveType.Kind elementKind, final int dimensions) {
        Objects.requireNonNull(elementKind);
        Preconditions.checkArgument(elementKind == IPrimitiveType.Kind.HALF ||
                        elementKind == IPrimitiveType.Kind.FLOAT ||
                        elementKind == IPrimitiveType.Kind.DOUBLE,
                "the elements of a vector must be half, float, or double");
        this.isNullable = isNullable;
        this.elementKind = elementKind;
        this.dimensions = dimensions;
    }

    @Override
    public boolean isNullable() {
        return isNullable;
    }

    /**
     * {@return the type of the elements}
     */
    @Nonnull
    public IPrimitiveType.Kind elementKind() {
        return elementKind;
    }

    /**
     * {@return the number of elements}
     */
    public int dimensions() {
        return dimensions;
    }

    @Nonnull
    @Override
    public IVectorType withNullability(final boolean isNullable) {
        return new IVectorType(isNullable, elementKind, dimensions);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IVectorType that && isNullable == that.isNullable && elementKind == that.elementKind
                && dimensions == that.dimensions;
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        // the keyword and not the enum constant: the hash code of an enum constant is its identity hash code
        return Objects.hash("vector type", isNullable, elementKind.getKeyword(), dimensions);
    }

    @Nonnull
    @Override
    public String toString() {
        return "vector<" + elementKind.getKeyword() + ", " + dimensions + ">" + ParserHelpers.nullableSuffix(isNullable);
    }
}
