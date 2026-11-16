/*
 * IIfFragment.java
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
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * An if, {@code if c { … } else { … }}: a statement that runs its then block if its condition is true, and its else
 * block, if it has one, otherwise. The condition is a boolean that is not nullable, since there is no promotion. An
 * {@code else if} is an else block whose only statement is another if. An if is a statement, not an expression, so it
 * has no value.
 */
@API(API.Status.EXPERIMENTAL)
public final class IIfFragment implements IControlFlowFragment {
    @Nonnull
    private final IExpressionFragment condition;
    @Nonnull
    private final IBlockFragment thenBlock;
    @Nullable
    private final IBlockFragment elseBlock;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates an if.
     *
     * @param condition the condition, which must be of type {@code boolean}
     * @param thenBlock the block that runs if the condition is true
     * @param elseBlock the block that runs if the condition is false, or {@code null} if there is none
     */
    public IIfFragment(@Nonnull final IExpressionFragment condition, @Nonnull final IBlockFragment thenBlock,
                       @Nullable final IBlockFragment elseBlock) {
        this.condition = Objects.requireNonNull(condition);
        this.thenBlock = Objects.requireNonNull(thenBlock);
        this.elseBlock = elseBlock;
        Preconditions.checkArgument(condition.type().equals(new IPrimitiveType(IPrimitiveType.Kind.BOOLEAN, false)),
                "the condition of an if must be a boolean");
    }

    /**
     * {@return the condition}
     */
    @Nonnull
    public IExpressionFragment condition() {
        return condition;
    }

    /**
     * {@return the block that runs if the condition is true}
     */
    @Nonnull
    public IBlockFragment thenBlock() {
        return thenBlock;
    }

    /**
     * {@return the block that runs if the condition is false, or empty if there is none}
     */
    @Nonnull
    public Optional<IBlockFragment> elseBlock() {
        return Optional.ofNullable(elseBlock);
    }

    @Nonnull
    @Override
    public List<IBlockFragment> blocks() {
        return elseBlock == null ? ImmutableList.of(thenBlock) : ImmutableList.of(thenBlock, elseBlock);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IIfFragment that && condition.equals(that.condition)
                && thenBlock.equals(that.thenBlock) && Objects.equals(elseBlock, that.elseBlock);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("if", condition, thenBlock, elseBlock);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IIfFragment[condition=" + condition + ", thenBlock=" + thenBlock + ", elseBlock=" + elseBlock + "]";
    }
}
