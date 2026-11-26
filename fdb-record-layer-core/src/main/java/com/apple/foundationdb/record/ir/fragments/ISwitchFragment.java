/*
 * ISwitchFragment.java
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
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A switch, {@code switch c { case .RED, .GREEN: … default: … }}: a statement that runs the block of the case that
 * names the value of an enum value, or its default block if no case names it. The value is not nullable. The cases name
 * every value of the enum type, or there is a default block, but not both, since the default block would then never
 * run. No value is named twice. A case does not fall through to the next one. A switch is a statement, not an
 * expression, so it has no value.
 */
@API(API.Status.EXPERIMENTAL)
public final class ISwitchFragment implements IControlFlowFragment {
    @Nonnull
    private final IExpressionFragment value;
    @Nonnull
    private final ImmutableList<Case> cases;
    @Nullable
    private final IBlockFragment defaultBlock;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a switch.
     *
     * @param value the enum value that selects the case, which must be of an enum type that is not nullable
     * @param cases the cases in order, at least one; together they name each value of the enum type at most once
     * @param defaultBlock the block that runs if no case names the value, or {@code null} if the cases name every
     *        value of the enum type
     */
    public ISwitchFragment(@Nonnull final IExpressionFragment value, @Nonnull final List<Case> cases,
                           @Nullable final IBlockFragment defaultBlock) {
        this.value = Objects.requireNonNull(value);
        this.cases = ImmutableList.copyOf(cases);
        this.defaultBlock = defaultBlock;
        Preconditions.checkArgument(value.type() instanceof IEnumType enumType && !enumType.isNullable(),
                "a switch needs an enum value that is not nullable");
        Preconditions.checkArgument(!this.cases.isEmpty(), "a switch needs a case");
        final List<IEnumType.Value> enumValues = ((IEnumType)value.type()).values();
        final List<IEnumType.Value> named = this.cases.stream()
                .flatMap(switchCase -> switchCase.values().stream())
                .collect(ImmutableList.toImmutableList());
        Preconditions.checkArgument(enumValues.containsAll(named),
                "the cases of a switch name values of the enum type of its value");
        Preconditions.checkArgument(ImmutableSet.copyOf(named).size() == named.size(),
                "the cases of a switch name each value at most once");
        Preconditions.checkArgument(named.containsAll(enumValues) == (defaultBlock == null),
                "a switch has a default case if and only if its cases do not name every value of its enum type");
    }

    /**
     * {@return the enum value that selects the case}
     */
    @Nonnull
    public IExpressionFragment value() {
        return value;
    }

    /**
     * {@return the cases in order}
     */
    @Nonnull
    public List<Case> cases() {
        return cases;
    }

    /**
     * {@return the block that runs if no case names the value, or empty if the cases name every value}
     */
    @Nonnull
    public Optional<IBlockFragment> defaultBlock() {
        return Optional.ofNullable(defaultBlock);
    }

    /**
     * Returns the blocks of the cases in order, followed by the default block if there is one.
     *
     * @return the blocks
     */
    @Nonnull
    @Override
    public List<IBlockFragment> blocks() {
        final ImmutableList.Builder<IBlockFragment> blocks = ImmutableList.builder();
        cases.forEach(switchCase -> blocks.add(switchCase.body()));
        if (defaultBlock != null) {
            blocks.add(defaultBlock);
        }
        return blocks.build();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ISwitchFragment that && value.equals(that.value) && cases.equals(that.cases)
                && Objects.equals(defaultBlock, that.defaultBlock);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("switch", value, cases, defaultBlock);
    }

    @Nonnull
    @Override
    public String toString() {
        return "ISwitchFragment[value=" + value + ", cases=" + cases + ", defaultBlock=" + defaultBlock + "]";
    }

    /**
     * A case of a switch, {@code case .RED, .GREEN: …}: the enum values that it names, and the block that runs if the
     * value of the switch is one of them.
     */
    public static final class Case implements IFragment {
        @Nonnull
        private final ImmutableList<IEnumType.Value> values;
        @Nonnull
        private final IBlockFragment body;
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        /**
         * Creates a case.
         *
         * @param values the enum values that the case names, in order, at least one
         * @param body the block that runs if the value of the switch is one of the values
         */
        public Case(@Nonnull final List<IEnumType.Value> values, @Nonnull final IBlockFragment body) {
            this.values = ImmutableList.copyOf(values);
            this.body = Objects.requireNonNull(body);
            Preconditions.checkArgument(!this.values.isEmpty(), "a case of a switch names a value");
        }

        /**
         * {@return the enum values that the case names, in order}
         */
        @Nonnull
        public List<IEnumType.Value> values() {
            return values;
        }

        /**
         * {@return the block that runs if the value of the switch is one of the values}
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
            return other instanceof Case that && values.equals(that.values) && body.equals(that.body);
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        private int computeHashCode() {
            return Objects.hash("switch case", values, body);
        }

        @Nonnull
        @Override
        public String toString() {
            return "Case[values=" + values + ", body=" + body + "]";
        }
    }
}
