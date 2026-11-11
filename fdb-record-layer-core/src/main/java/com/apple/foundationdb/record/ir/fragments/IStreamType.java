/*
 * IStreamType.java
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
 * The type of a stream of values, {@code stream<record<id: bigint>>}. A stream is a relation, and a relation is never
 * null, so stream types are never nullable.
 */
@API(API.Status.EXPERIMENTAL)
public final class IStreamType implements IType {
    @Nonnull
    private final IType elementType;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a stream type.
     *
     * @param elementType the type of the values in the stream, which cannot be {@code void}
     */
    public IStreamType(@Nonnull final IType elementType) {
        this.elementType = Objects.requireNonNull(elementType);
        Preconditions.checkArgument(!elementType.isVoid(), "void is only a result type");
    }

    /**
     * {@return the type of the values in the stream}
     */
    @Nonnull
    public IType elementType() {
        return elementType;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Nonnull
    @Override
    public IStreamType withNullability(final boolean isNullable) {
        Preconditions.checkArgument(!isNullable, "a stream type cannot be nullable");
        return this;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IStreamType that && elementType.equals(that.elementType);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("stream type", elementType);
    }

    @Nonnull
    @Override
    public String toString() {
        return "stream<" + elementType + ">";
    }
}
