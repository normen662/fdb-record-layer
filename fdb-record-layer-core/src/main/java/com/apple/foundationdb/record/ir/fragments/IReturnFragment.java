/*
 * IReturnFragment.java
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
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A return, {@code return x} or {@code return}: a statement that leaves the function or the closure that it is in. With
 * a value, the value is the result. Without a value, the result type is {@code void}. A return in a closure leaves the
 * closure, as in Swift. A return is a statement of the body of a function or a closure, also in the blocks of its
 * statements of control flow, but not of a program or a stream block: a stream block ends with a yield break, see
 * {@link IYieldBreakFragment}. A return does not complete normally, so no statement can follow it in its block.
 */
@API(API.Status.EXPERIMENTAL)
public final class IReturnFragment implements IStatementFragment {
    @Nullable
    private final IExpressionFragment value;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a return.
     *
     * @param value the value that the return gives back, which cannot be of type {@code void}, or {@code null} if the
     *        result type is {@code void}
     */
    public IReturnFragment(@Nullable final IExpressionFragment value) {
        this.value = value;
        Preconditions.checkArgument(value == null || !value.type().isVoid(), "void is only a result type");
    }

    /**
     * {@return the value that the return gives back, or empty if the result type is {@code void}}
     */
    @Nonnull
    public Optional<IExpressionFragment> value() {
        return Optional.ofNullable(value);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IReturnFragment that && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("return", value);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IReturnFragment[value=" + value + "]";
    }
}
