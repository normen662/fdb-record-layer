/*
 * IGenericType.java
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

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A type parameter of a generic function, e.g. {@code T} in {@code func id<T>(x: T): T}. The type it stands for is
 * known only where the function is used. Only a scope that declares the type parameter gives the name a meaning.
 * Equality compares the name and the nullability, so the type parameters {@code T} of two different functions are
 * equal.
 */
@API(API.Status.EXPERIMENTAL)
public final class IGenericType implements IType {
    @Nonnull
    private final String name;
    private final boolean isNullable;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a type parameter.
     *
     * @param name the name of the type parameter
     * @param isNullable whether values of this type can be null
     */
    public IGenericType(@Nonnull final String name, final boolean isNullable) {
        this.name = Objects.requireNonNull(name);
        this.isNullable = isNullable;
        ParserHelpers.checkWritableName(name);
    }

    /**
     * {@return the name of the type parameter}
     */
    @Nonnull
    public String name() {
        return name;
    }

    @Override
    public boolean isNullable() {
        return isNullable;
    }

    @Nonnull
    @Override
    public IGenericType withNullability(final boolean isNullable) {
        return new IGenericType(name, isNullable);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IGenericType that && name.equals(that.name) && isNullable == that.isNullable;
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("generic type", name, isNullable);
    }

    @Nonnull
    @Override
    public String toString() {
        return ParserHelpers.writeName(name) + ParserHelpers.nullableSuffix(isNullable);
    }
}
