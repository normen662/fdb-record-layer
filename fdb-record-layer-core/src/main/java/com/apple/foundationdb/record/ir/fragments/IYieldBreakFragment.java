/*
 * IYieldBreakFragment.java
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
 * A yield break, {@code yield break} or {@code yield break r}: a statement of a stream block that leaves the block, as
 * {@code break} leaves a loop in Java. Without a reason, it ends the stream, as {@code yield break} does in C#: the
 * stream has no more elements, as when the block runs to its end. With a reason, it stops the stream for that reason
 * instead, so that its consumer can resume the stream from its continuation, at the yield break. The reason is a
 * {@code stopReason}, see {@link IConsumeFragment#STOP_REASON_TYPE}. A yield break is a statement of a stream block, in
 * the places where a yield can be, see {@link IYieldFragment}.
 */
@API(API.Status.EXPERIMENTAL)
public final class IYieldBreakFragment implements IStatementFragment {
    @Nullable
    private final IExpressionFragment reason;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a yield break.
     *
     * @param reason the reason why the stream stops, which must be a {@code stopReason}, or {@code null} if the stream
     *        ends
     */
    public IYieldBreakFragment(@Nullable final IExpressionFragment reason) {
        this.reason = reason;
        Preconditions.checkArgument(reason == null || reason.type().equals(IConsumeFragment.STOP_REASON_TYPE),
                "the reason of a yield break must be a stopReason");
    }

    /**
     * {@return the reason why the stream stops, or empty if the stream ends}
     */
    @Nonnull
    public Optional<IExpressionFragment> reason() {
        return Optional.ofNullable(reason);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IYieldBreakFragment that && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("yield break", reason);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IYieldBreakFragment[reason=" + reason + "]";
    }
}
