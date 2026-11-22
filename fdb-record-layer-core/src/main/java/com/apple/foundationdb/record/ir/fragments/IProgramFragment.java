/*
 * IProgramFragment.java
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
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A program, the construct that parsing FRL-IR starts from: a sequence of statements, separated by line breaks or
 * semicolons as in a block. A program can belong to a package, which its first statement then names. This is a
 * placeholder that carries the statements; what a program means is yet to be designed.
 */
@API(API.Status.EXPERIMENTAL)
public final class IProgramFragment implements IFragment {
    @Nonnull
    private final ImmutableList<IStatementFragment> statements;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a program.
     *
     * @param statements the statements in order; only the first one can be a package statement, and none can yield,
     *        consume, or return, also not in the blocks of a statement of control flow, since a yield, a yield break,
     *        and a consume belong to a stream block, and a return to a function or a closure
     */
    public IProgramFragment(@Nonnull final List<IStatementFragment> statements) {
        this.statements = ImmutableList.copyOf(statements);
        Preconditions.checkArgument(
                this.statements.stream().skip(1).noneMatch(IPackageStatementFragment.class::isInstance),
                "a package statement can only be the first statement of a program");
        Preconditions.checkArgument(!IBlockFragment.hasStreamStatements(this.statements),
                "a yield, a yield break, and a consume must be in a stream block");
        Preconditions.checkArgument(!IBlockFragment.hasReturns(this.statements),
                "a return must be in a function or a closure");
    }

    /**
     * {@return the statements in order}
     */
    @Nonnull
    public List<IStatementFragment> statements() {
        return statements;
    }

    /**
     * {@return the package statement of the program, its first statement, or empty if the program has none}
     */
    @Nonnull
    public Optional<IPackageStatementFragment> packageStatement() {
        return statements.stream()
                .findFirst()
                .filter(IPackageStatementFragment.class::isInstance)
                .map(IPackageStatementFragment.class::cast);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IProgramFragment that && statements.equals(that.statements);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("program", statements);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IProgramFragment[statements=" + statements + "]";
    }
}
