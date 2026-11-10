/*
 * IEnumType.java
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

package com.apple.foundationdb.record.ir.types;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ir.fragment.IFragment;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An enum type, {@code enum<RED = 0, GREEN = 1>}. Every value states its number, because the numbers are part of how
 * enum values are stored and need not follow the order of the values. Enum types have no name of their own.
 */
@API(API.Status.EXPERIMENTAL)
public final class IEnumType implements IType {
    private final boolean isNullable;
    @Nonnull
    private final ImmutableList<Value> values;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates an enum type.
     *
     * @param isNullable whether values of this type can be null
     * @param values the values in order; there is at least one
     */
    public IEnumType(final boolean isNullable, @Nonnull final List<Value> values) {
        this.isNullable = isNullable;
        this.values = ImmutableList.copyOf(values);
        Preconditions.checkArgument(!values.isEmpty(), "an enum type needs at least one value");
    }

    @Override
    public boolean isNullable() {
        return isNullable;
    }

    /**
     * {@return the values in order}
     */
    @Nonnull
    public List<Value> values() {
        return values;
    }

    @Nonnull
    @Override
    public IEnumType withNullability(final boolean isNullable) {
        return new IEnumType(isNullable, values);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IEnumType that && isNullable == that.isNullable && values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("enum type", isNullable, values);
    }

    @Nonnull
    @Override
    public String toString() {
        return values.stream().map(Value::toString).collect(Collectors.joining(", ", "enum<", ">"))
                + ParserHelpers.nullableSuffix(isNullable);
    }

    /**
     * A value of an enum type.
     */
    public static final class Value implements IFragment {
        @Nonnull
        private final String name;
        private final int number;
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        /**
         * Creates an enum value.
         *
         * @param name the name of the value
         * @param number the number of the value in the stored representation
         */
        public Value(@Nonnull final String name, final int number) {
            this.name = Objects.requireNonNull(name);
            this.number = number;
            ParserHelpers.checkWritableName(name);
        }

        /**
         * {@return the name of the value}
         */
        @Nonnull
        public String name() {
            return name;
        }

        /**
         * {@return the number of the value in the stored representation}
         */
        public int number() {
            return number;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            return other instanceof Value that && name.equals(that.name) && number == that.number;
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        private int computeHashCode() {
            return Objects.hash("enum type value", name, number);
        }

        @Nonnull
        @Override
        public String toString() {
            return ParserHelpers.writeName(name) + " = " + number;
        }
    }
}
