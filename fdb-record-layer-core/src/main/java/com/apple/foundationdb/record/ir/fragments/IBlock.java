/*
 * IBlock.java
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
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The body of a function definition or a closure: a sequence of statements, separated by line breaks or semicolons. The
 * value of a block is the value of its last statement if that statement is an expression, as in a Kotlin lambda; a
 * block whose last statement is not an expression, or that has no statements, has no value.
 */
@API(API.Status.EXPERIMENTAL)
public final class IBlock implements IFragment {
    @Nonnull
    private final ImmutableList<IStatement> statements;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a block.
     *
     * @param statements the statements in order
     */
    public IBlock(@Nonnull final List<IStatement> statements) {
        this.statements = ImmutableList.copyOf(statements);
    }

    /**
     * {@return the statements in order}
     */
    @Nonnull
    public List<IStatement> statements() {
        return statements;
    }

    /**
     * {@return the expression whose value is the value of this block: its last statement, if that is an expression}
     */
    @Nonnull
    public Optional<IExpression> value() {
        return statements.isEmpty() || !(statements.get(statements.size() - 1) instanceof IExpression value)
                ? Optional.empty()
                : Optional.of(value);
    }

    /**
     * Returns whether this block can implement a function or a closure of the given result type. A {@code void}
     * function discards the value of its body, if there is one; any other function returns it, so the value must be of
     * the result type.
     *
     * @param resultType the result type of the function or the closure
     * @return {@code true} if the result type is {@code void}, or if this block has a value of that type
     */
    public boolean isBodyOf(@Nonnull final IType resultType) {
        return resultType.isVoid() || value().map(IExpression::type).filter(resultType::equals).isPresent();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IBlock that && statements.equals(that.statements);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("block", statements);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IBlock[statements=" + statements + "]";
    }
}
