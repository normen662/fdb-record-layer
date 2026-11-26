/*
 * IBlockFragment.java
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
import java.util.stream.Stream;

/**
 * The body of a function definition, a closure, or a stream block, or a block of a statement of control flow: a
 * sequence of statements, separated by line breaks or semicolons. As in Swift, the value of a block is its only
 * statement if the block has exactly one statement and that statement is an expression. Any other block has no value;
 * the body of a function or a closure then gives its result with returns, see {@link IReturnFragment}. No statement of
 * a block can follow a statement that does not complete normally, see {@link #completesNormally()}, since it would
 * never run.
 */
@API(API.Status.EXPERIMENTAL)
public final class IBlockFragment implements IFragment {
    @Nonnull
    private final ImmutableList<IStatementFragment> statements;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a block.
     *
     * @param statements the statements in order; only the last one can be a statement that does not complete normally
     */
    public IBlockFragment(@Nonnull final List<IStatementFragment> statements) {
        this.statements = ImmutableList.copyOf(statements);
        Preconditions.checkArgument(this.statements.subList(0, Math.max(0, this.statements.size() - 1))
                        .stream()
                        .allMatch(IBlockFragment::completesNormally),
                "no statement can follow a statement that does not complete normally, such as a return");
    }

    /**
     * {@return the statements in order}
     */
    @Nonnull
    public List<IStatementFragment> statements() {
        return statements;
    }

    /**
     * {@return the expression whose value is the value of this block: its only statement, if that is an expression}
     */
    @Nonnull
    public Optional<IExpressionFragment> value() {
        return statements.size() != 1 || !(statements.get(0) instanceof IExpressionFragment value)
                ? Optional.empty()
                : Optional.of(value);
    }

    /**
     * Returns the yields of this block, the statements that add a value to the stream of the stream block that this
     * block is the body of: its yields, and those in the blocks of its statements of control flow, which run as part of
     * this block. The yields of a stream block among the statements belong to that stream block, and a function or a
     * closure has none.
     *
     * @return the yields among the statements, in order
     */
    @Nonnull
    public List<IYieldFragment> yields() {
        return runningStatements(statements)
                .filter(IYieldFragment.class::isInstance)
                .map(IYieldFragment.class::cast)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Returns the returns of this block, the statements that leave the function or the closure that this block is the
     * body of: its returns, and those in the blocks of its statements of control flow. The returns of a function, a
     * closure, or a stream block among the statements belong to that construct.
     *
     * @return the returns among the statements, in order
     */
    @Nonnull
    public List<IReturnFragment> returns() {
        return runningStatements(statements)
                .filter(IReturnFragment.class::isInstance)
                .map(IReturnFragment.class::cast)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Returns whether this block can complete normally, that is, whether a path through it reaches its end. A block
     * cannot complete normally if its last statement cannot, see {@link #completesNormally(IStatementFragment)}. So the
     * body of a function or a closure that cannot complete normally gives its result with a return on every path.
     *
     * @return {@code true} if a path through this block reaches its end
     */
    public boolean completesNormally() {
        return statements.isEmpty() || completesNormally(statements.get(statements.size() - 1));
    }

    /**
     * Returns whether a statement can complete normally, so that a statement after it can run. A return and a yield
     * break cannot. An if with an else cannot if both of its blocks cannot, and a switch cannot if none of its blocks
     * can, since it always runs one of them. A consume cannot if it has an end case that cannot: its stopped case
     * cannot, and its value case returns to its loop. Every other statement can: a for loop, since its array can be
     * empty, and a stream block or a closure, whose returns and yield breaks are their own.
     *
     * @param statement the statement
     * @return {@code true} if the statement can complete normally
     */
    static boolean completesNormally(@Nonnull final IStatementFragment statement) {
        if (statement instanceof IReturnFragment || statement instanceof IYieldBreakFragment) {
            return false;
        }
        if (statement instanceof IIfFragment conditional) {
            return conditional.elseBlock().isEmpty() || conditional.thenBlock().completesNormally()
                    || conditional.elseBlock().get().completesNormally();
        }
        if (statement instanceof ISwitchFragment switchStatement) {
            return switchStatement.blocks().stream().anyMatch(IBlockFragment::completesNormally);
        }
        if (statement instanceof IConsumeFragment consume) {
            return consume.endCase().isEmpty() || consume.endCase().get().body().completesNormally();
        }
        return true;
    }

    /**
     * Returns whether this block can implement a function or a closure of the given result type. If the result type is
     * {@code void}, the returns have no value, and the block discards the value of a single expression. Otherwise each
     * return has a value of the result type, and the block is a single expression of that type or cannot complete
     * normally, so that every path gives a value. Neither kind of body can yield or consume, also not in the blocks of
     * its statements of control flow: a yield belongs to a stream block, and a consume stops the stream block it is in
     * when the consumed stream stops, while a function or a closure can be called when no stream block is running.
     *
     * @param resultType the result type of the function or the closure
     * @return {@code true} if this block does not yield or consume, and if it gives a value of the result type on every
     *         path, or the result type is {@code void} and no return has a value
     */
    public boolean isBodyOf(@Nonnull final IType resultType) {
        if (hasStreamStatements(statements)) {
            return false;
        }
        final List<IReturnFragment> returns = returns();
        if (resultType.isVoid()) {
            return returns.stream().allMatch(returnStatement -> returnStatement.value().isEmpty());
        }
        return returns.stream().allMatch(returnStatement -> returnStatement.value()
                        .map(IExpressionFragment::type)
                        .filter(resultType::equals)
                        .isPresent())
                && (value().map(IExpressionFragment::type).filter(resultType::equals).isPresent()
                        || !completesNormally());
    }

    /**
     * Returns whether statements can only be those of a stream block: whether they, or the statements in the blocks of
     * their statements of control flow, yield, break the stream with a yield break, or consume a stream. A yield and a
     * yield break belong to a stream block, and a consume stops the stream block it is in when the consumed stream
     * stops, while a function, a closure, or a program can run when no stream block is running.
     *
     * @param statements the statements
     * @return {@code true} if one of the statements, or of the statements in their blocks of control flow, is a yield,
     *         a yield break, or a consume
     */
    static boolean hasStreamStatements(@Nonnull final List<IStatementFragment> statements) {
        return runningStatements(statements).anyMatch(statement -> statement instanceof IYieldFragment
                || statement instanceof IYieldBreakFragment || statement instanceof IConsumeFragment);
    }

    /**
     * Returns whether statements, or the statements in the blocks of their statements of control flow, return. A
     * return belongs to a function or a closure, so a program cannot have one.
     *
     * @param statements the statements
     * @return {@code true} if one of the statements, or of the statements in their blocks of control flow, is a return
     */
    static boolean hasReturns(@Nonnull final List<IStatementFragment> statements) {
        return runningStatements(statements).anyMatch(IReturnFragment.class::isInstance);
    }

    /**
     * Returns statements, each followed by the statements in its blocks if it is a statement of control flow,
     * recursively: the statements that run as part of a block that has the given statements. A function, a closure, or
     * a stream block runs its block when the function or closure is called, or as the stream is consumed, so its
     * statements are not among them.
     */
    @Nonnull
    private static Stream<IStatementFragment> runningStatements(@Nonnull final List<IStatementFragment> statements) {
        return statements.stream().flatMap(statement -> statement instanceof IControlFlowFragment controlFlow
                ? Stream.concat(Stream.of(statement), controlFlow.blocks()
                        .stream()
                        .flatMap(block -> runningStatements(block.statements())))
                : Stream.of(statement));
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IBlockFragment that && statements.equals(that.statements);
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
        return "IBlockFragment[statements=" + statements + "]";
    }
}
