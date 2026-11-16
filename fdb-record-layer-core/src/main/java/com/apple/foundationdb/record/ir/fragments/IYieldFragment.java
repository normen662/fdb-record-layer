/*
 * IYieldFragment.java
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
 * A yield, {@code yield x}: a statement of a stream block, {@code stream { … }}, that adds the value of {@code x} to
 * the stream of the block, as {@code yield return} does in C#. The block runs as the stream is consumed, so every yield
 * is a point at which it stops until the next element is asked for, see {@link IStreamFragment}. A yield belongs to the
 * innermost stream block around it and is a statement of that block itself: a function or a closure cannot yield,
 * since it can be called when no stream block is running.
 */
@API(API.Status.EXPERIMENTAL)
public final class IYieldFragment implements IStatement {
    @Nonnull
    private final IExpression value;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a yield.
     *
     * @param value the expression whose value is yielded, which cannot be of type {@code void}
     */
    public IYieldFragment(@Nonnull final IExpression value) {
        this.value = Objects.requireNonNull(value);
        Preconditions.checkArgument(!value.type().isVoid(), "void is only a result type");
    }

    /**
     * {@return the expression whose value is yielded}
     */
    @Nonnull
    public IExpression value() {
        return value;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IYieldFragment that && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("yield", value);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IYieldFragment[value=" + value + "]";
    }
}
