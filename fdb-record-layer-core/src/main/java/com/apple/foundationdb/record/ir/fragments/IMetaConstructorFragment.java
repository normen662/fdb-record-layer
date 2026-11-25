/*
 * IMetaConstructorFragment.java
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
 * A meta constructor, {@code meta.of<record<id: bigint>>()}: an expression whose value is the type token of its type
 * argument, and whose type is the meta type of that type argument, {@code meta<record<id: bigint>>}. It is the only
 * way to make a token, see {@link IMetaType}. The type argument can be a type parameter of an enclosing function,
 * since the runtime specializes a function before it runs it, so the type is known when the token is made.
 */
@API(API.Status.EXPERIMENTAL)
public final class IMetaConstructorFragment implements IExpressionFragment {
    @Nonnull
    private final IType typeArgument;
    @Nonnull
    private final IMetaType type;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a meta constructor.
     *
     * @param typeArgument the type that the token stands for, which cannot be {@code void}
     */
    public IMetaConstructorFragment(@Nonnull final IType typeArgument) {
        this.typeArgument = Objects.requireNonNull(typeArgument);
        this.type = new IMetaType(typeArgument);
    }

    /**
     * {@return the type that the token stands for}
     */
    @Nonnull
    public IType typeArgument() {
        return typeArgument;
    }

    /**
     * Returns the type of the meta constructor, the meta type of its type argument.
     *
     * @return the type of the meta constructor
     */
    @Nonnull
    @Override
    public IMetaType type() {
        return type;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IMetaConstructorFragment that && typeArgument.equals(that.typeArgument);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("meta constructor", typeArgument);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IMetaConstructorFragment[typeArgument=" + typeArgument + "]";
    }
}
