/*
 * IForFragment.java
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

/**
 * A for loop, {@code for x in xs { … }}: a statement that runs its body for each element of an array, in order, with
 * the element as the value of the loop variable. The array is not nullable, since there is no promotion. An array is in
 * memory as a whole, so a for loop ends only after its last element; a stream, which can stop before its end, is
 * consumed with an {@link IConsumeFragment} instead.
 */
@API(API.Status.EXPERIMENTAL)
public final class IForFragment implements IControlFlowFragment {
    @Nonnull
    private final String variable;
    @Nonnull
    private final IExpressionFragment sequence;
    @Nonnull
    private final IBlockFragment body;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a for loop.
     *
     * @param variable the name of the loop variable
     * @param sequence the array whose elements the loop variable takes, which must be of an array type that is not
     *        nullable
     * @param body the block that runs for each element
     */
    public IForFragment(@Nonnull final String variable, @Nonnull final IExpressionFragment sequence,
                        @Nonnull final IBlockFragment body) {
        this.variable = Objects.requireNonNull(variable);
        this.sequence = Objects.requireNonNull(sequence);
        this.body = Objects.requireNonNull(body);
        Preconditions.checkArgument(sequence.type() instanceof IArrayType arrayType && !arrayType.isNullable(),
                "a for loop must iterate over an array that is not nullable");
    }

    /**
     * {@return the name of the loop variable}
     */
    @Nonnull
    public String variable() {
        return variable;
    }

    /**
     * Returns the type of the loop variable, the element type of the array.
     *
     * @return the type of the loop variable
     */
    @Nonnull
    public IType variableType() {
        // the constructor checks that the sequence is an array
        return ((IArrayType)sequence.type()).elementType();
    }

    /**
     * {@return the array whose elements the loop variable takes}
     */
    @Nonnull
    public IExpressionFragment sequence() {
        return sequence;
    }

    /**
     * {@return the block that runs for each element}
     */
    @Nonnull
    public IBlockFragment body() {
        return body;
    }

    @Nonnull
    @Override
    public List<IBlockFragment> blocks() {
        return ImmutableList.of(body);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IForFragment that && variable.equals(that.variable) && sequence.equals(that.sequence)
                && body.equals(that.body);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("for", variable, sequence, body);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IForFragment[variable=" + variable + ", sequence=" + sequence + ", body=" + body + "]";
    }
}
