/*
 * IMetaType.java
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
 * The type of a type token, {@code meta<record<id: bigint>>}: an opaque value that stands for a type, which only
 * {@link IMetaConstructorFragment} makes. A program can do nothing with a token but pass it on. A token fixes the type
 * parameter of a function that takes it, as {@code as: meta<T>} does in
 * {@code scan<T>(_ name: string, as: meta<T>) -> stream<T>}, and the runtime gives its type to the external function
 * that receives it. A token is never null, so meta types are never nullable. Cascades has no such type.
 */
@API(API.Status.EXPERIMENTAL)
public final class IMetaType implements IType {
    @Nonnull
    private final IType type;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a meta type.
     *
     * @param type the type that the tokens of this type stand for, which cannot be {@code void}
     */
    public IMetaType(@Nonnull final IType type) {
        this.type = Objects.requireNonNull(type);
        Preconditions.checkArgument(!type.isVoid(), "void is only a result type");
    }

    /**
     * {@return the type that the tokens of this type stand for}
     */
    @Nonnull
    public IType type() {
        return type;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Nonnull
    @Override
    public IMetaType withNullability(final boolean isNullable) {
        Preconditions.checkArgument(!isNullable, "a meta type cannot be nullable");
        return this;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IMetaType that && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("meta type", type);
    }

    @Nonnull
    @Override
    public String toString() {
        return "meta<" + type + ">";
    }
}
