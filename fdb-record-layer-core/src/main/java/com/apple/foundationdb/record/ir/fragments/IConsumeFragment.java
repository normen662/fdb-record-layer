/*
 * IConsumeFragment.java
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
 * A consume, {@code consume s { case .value(x): … case .end: … case .stopped(r): … }}: a statement that pulls the
 * elements of a stream in a loop, with a case for each answer of the stream.
 * <ul>
 *     <li>The value case runs for every element, with the element as the value of its binding, and then the loop pulls
 *     the next element.</li>
 *     <li>The end case, if there is one, runs once the stream has no more elements, and then the loop is left. Without
 *     an end case, the loop is left at the end of the stream.</li>
 *     <li>The stopped case runs if the stream stops before its end, at a time, scan, or byte limit for instance, with
 *     the reason, a {@linkplain #STOP_REASON_TYPE stopReason}, as the value of its binding. Every consume has one, and
 *     its statements end with a yield break on every path, because the reason has to be communicated outward: a yield
 *     break with a reason stops the stream block that the consume is in, and one without a reason ends its stream. A
 *     stop is not the end: the stream can be resumed from its continuation, which is part of the continuation of the
 *     stream block. A resume continues after the yield break, and the loop then pulls the next answer of the resumed
 *     stream, as after the value case.</li>
 * </ul>
 * Only a stream block can stop and be resumed, so a consume is a statement of a stream block, in the places where a
 * yield can be.
 */
@API(API.Status.EXPERIMENTAL)
public final class IConsumeFragment implements IControlFlowFragment {
    /**
     * The type of the reason why a stream stops before its end, which a stopped case binds and a yield break passes
     * on: the enum of the reasons why a cursor of the record layer stops before its end, the values of
     * {@link com.apple.foundationdb.record.RecordCursor.NoNextReason} other than {@code SOURCE_EXHAUSTED}, with their
     * names and numbered by their ordinals there. {@code SOURCE_EXHAUSTED} is left out because the end of a stream is
     * not a stop: a stream block ends its stream by leaving its body, at the end of the body or with a yield break
     * without a reason, and a consume answers the end with its end case.
     */
    @Nonnull
    public static final IEnumType STOP_REASON_TYPE = new IEnumType(false, ImmutableList.of(
            new IEnumType.Value("RETURN_LIMIT_REACHED", 1),
            new IEnumType.Value("TIME_LIMIT_REACHED", 2),
            new IEnumType.Value("SCAN_LIMIT_REACHED", 3),
            new IEnumType.Value("BYTE_LIMIT_REACHED", 4)));

    /**
     * The type alias {@code stopReason}, the name of {@link #STOP_REASON_TYPE} in a program. FRL-IR declares it in the
     * outermost scope, the scope of the statements of a program, so no program needs to spell out the enum.
     */
    @Nonnull
    public static final ITypeAliasFragment STOP_REASON_ALIAS = new ITypeAliasFragment("stopReason", STOP_REASON_TYPE);

    @Nonnull
    private final IExpressionFragment stream;
    @Nonnull
    private final Case valueCase;
    @Nullable
    private final Case endCase;
    @Nonnull
    private final Case stoppedCase;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a consume.
     *
     * @param stream the stream whose elements the consume pulls, which must be of a stream type
     * @param valueCase the case that runs for every element, which binds the element
     * @param endCase the case that runs at the end of the stream, which binds nothing, or {@code null} if there is none
     * @param stoppedCase the case that runs if the stream stops before its end, which binds the reason, and whose
     *        statements end with a yield break on every path
     */
    public IConsumeFragment(@Nonnull final IExpressionFragment stream, @Nonnull final Case valueCase,
                            @Nullable final Case endCase, @Nonnull final Case stoppedCase) {
        this.stream = Objects.requireNonNull(stream);
        this.valueCase = Objects.requireNonNull(valueCase);
        this.endCase = endCase;
        this.stoppedCase = Objects.requireNonNull(stoppedCase);
        Preconditions.checkArgument(stream.type() instanceof IStreamType, "a consume must consume a stream");
        Preconditions.checkArgument(valueCase.binding().isPresent()
                        && (endCase == null || endCase.binding().isEmpty())
                        && stoppedCase.binding().isPresent(),
                "the value case and the stopped case of a consume bind a name, and the end case binds none");
        // a stream block cannot return, so a stopped case that cannot complete normally ends with a yield break on
        // every path
        Preconditions.checkArgument(!stoppedCase.body().completesNormally(),
                "the stopped case of a consume must end with a yield break on every path");
    }

    /**
     * {@return the stream whose elements the consume pulls}
     */
    @Nonnull
    public IExpressionFragment stream() {
        return stream;
    }

    /**
     * Returns the element type of the stream, the type of the binding of the value case.
     *
     * @return the element type of the stream
     */
    @Nonnull
    public IType elementType() {
        // the constructor checks that the stream is a stream
        return ((IStreamType)stream.type()).elementType();
    }

    /**
     * {@return the case that runs for every element}
     */
    @Nonnull
    public Case valueCase() {
        return valueCase;
    }

    /**
     * {@return the case that runs at the end of the stream, or empty if there is none}
     */
    @Nonnull
    public Optional<Case> endCase() {
        return Optional.ofNullable(endCase);
    }

    /**
     * {@return the case that runs if the stream stops before its end}
     */
    @Nonnull
    public Case stoppedCase() {
        return stoppedCase;
    }

    @Nonnull
    @Override
    public List<IBlockFragment> blocks() {
        final ImmutableList.Builder<IBlockFragment> blocks = ImmutableList.builder();
        blocks.add(valueCase.body());
        if (endCase != null) {
            blocks.add(endCase.body());
        }
        blocks.add(stoppedCase.body());
        return blocks.build();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IConsumeFragment that && stream.equals(that.stream)
                && valueCase.equals(that.valueCase) && Objects.equals(endCase, that.endCase)
                && stoppedCase.equals(that.stoppedCase);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("consume", stream, valueCase, endCase, stoppedCase);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IConsumeFragment[stream=" + stream + ", valueCase=" + valueCase + ", endCase=" + endCase
                + ", stoppedCase=" + stoppedCase + "]";
    }

    /**
     * A case of a consume: the name it binds, if any, and the block that runs when the stream answers with the case.
     */
    public static final class Case implements IFragment {
        @Nullable
        private final String binding;
        @Nonnull
        private final IBlockFragment body;
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        /**
         * Creates a case.
         *
         * @param binding the name that the case binds, the element or the reason, or {@code null} if it binds none
         * @param body the block that runs for the case
         */
        public Case(@Nullable final String binding, @Nonnull final IBlockFragment body) {
            this.binding = binding;
            this.body = Objects.requireNonNull(body);
        }

        /**
         * {@return the name that the case binds, or empty if it binds none}
         */
        @Nonnull
        public Optional<String> binding() {
            return Optional.ofNullable(binding);
        }

        /**
         * {@return the block that runs for the case}
         */
        @Nonnull
        public IBlockFragment body() {
            return body;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            return other instanceof Case that && Objects.equals(binding, that.binding) && body.equals(that.body);
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        private int computeHashCode() {
            return Objects.hash("consume case", binding, body);
        }

        @Nonnull
        @Override
        public String toString() {
            return "Case[binding=" + binding + ", body=" + body + "]";
        }
    }
}
