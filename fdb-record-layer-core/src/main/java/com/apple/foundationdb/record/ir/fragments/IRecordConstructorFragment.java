/*
 * IRecordConstructorFragment.java
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
 * A record constructor, {@code (id: 1, name: "a")}: an expression whose value is a record, and whose type is the record
 * type of its fields, {@code record<id: int, name: string>}. Each field has a value. A field has a name if the text
 * writes a label before its value. The record type is nullable if the context of the constructor requires a nullable
 * record, as in {@code let r: record<int, int>? = (1, 2)}. A literal becomes nullable in the same way. The text writes
 * a record constructor in a short form, {@code (id: 1, "a")}, or in a long form, {@code record.of(id: 1, "a")}, which
 * gives the same fragment. Only the long form can write a record without fields, {@code record.of()}, and a record of
 * one field without a name, {@code record.of(1)}. Only its type arguments can give a field a number, as in
 * {@code record.of<id(1): int, string>(id: 1, "a")}. The Cascades planner builds records with
 * {@code RecordConstructorValue}.
 */
@API(API.Status.EXPERIMENTAL)
public final class IRecordConstructorFragment implements IExpressionFragment {
    private final boolean isNullable;
    @Nonnull
    private final ImmutableList<Field> fields;
    @Nonnull
    private final IRecordType type;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a record constructor.
     *
     * @param isNullable whether the type of the record is nullable
     * @param fields the fields in order, any number of them; the named fields must have distinct names
     */
    public IRecordConstructorFragment(final boolean isNullable, @Nonnull final List<Field> fields) {
        this.isNullable = isNullable;
        this.fields = ImmutableList.copyOf(fields);
        this.type = new IRecordType(isNullable, this.fields.stream()
                .map(field -> new IRecordType.Field(field.value().type(), field.name(), field.number()))
                .collect(ImmutableList.toImmutableList()));
    }

    /**
     * {@return whether the type of the record is nullable}
     */
    public boolean isNullable() {
        return isNullable;
    }

    /**
     * {@return the fields in order}
     */
    @Nonnull
    public List<Field> fields() {
        return fields;
    }

    /**
     * Returns the type of the record constructor: the record type of the types, the names, and the numbers of the
     * fields, in order.
     *
     * @return the type of the record constructor
     */
    @Nonnull
    @Override
    public IRecordType type() {
        return type;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IRecordConstructorFragment that && isNullable == that.isNullable
                && fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("record constructor", isNullable, fields);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IRecordConstructorFragment[isNullable=" + isNullable + ", fields=" + fields + "]";
    }

    /**
     * A field of a record constructor, {@code id: 1}: a value, a name if the text writes a label before the value, and
     * a number if the type arguments of the long form state one for the field.
     */
    public static final class Field implements IFragment {
        @Nonnull
        private final IExpressionFragment value;
        @Nonnull
        private final Optional<String> name;
        @Nonnull
        private final Optional<Integer> number;
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        /**
         * Creates a field.
         *
         * @param value the value of the field, which cannot be of type {@code void}
         * @param name the name of the field, if it has one
         * @param number the number of the field in the stored record, if it has one; only a named field can have one
         */
        public Field(@Nonnull final IExpressionFragment value, @Nonnull final Optional<String> name,
                     @Nonnull final Optional<Integer> number) {
            this.value = Objects.requireNonNull(value);
            this.name = Objects.requireNonNull(name);
            this.number = Objects.requireNonNull(number);
            name.ifPresent(ParserHelpers::checkWritableName);
            Preconditions.checkArgument(name.isPresent() || number.isEmpty(), "a field number requires a field name");
            Preconditions.checkArgument(!value.type().isVoid(), "void is only a result type");
        }

        /**
         * Creates a field without a name and a number.
         *
         * @param value the value of the field
         * @return a new field
         */
        @Nonnull
        public static Field unnamedOf(@Nonnull final IExpressionFragment value) {
            return new Field(value, Optional.empty(), Optional.empty());
        }

        /**
         * Creates a field with a name and without a number.
         *
         * @param value the value of the field
         * @param name the name of the field
         * @return a new field
         */
        @Nonnull
        public static Field of(@Nonnull final IExpressionFragment value, @Nonnull final String name) {
            return new Field(value, Optional.of(name), Optional.empty());
        }

        /**
         * Creates a field with a name and a number.
         *
         * @param value the value of the field
         * @param name the name of the field
         * @param number the number of the field in the stored record
         * @return a new field
         */
        @Nonnull
        public static Field of(@Nonnull final IExpressionFragment value, @Nonnull final String name, final int number) {
            return new Field(value, Optional.of(name), Optional.of(number));
        }

        /**
         * {@return the value of the field}
         */
        @Nonnull
        public IExpressionFragment value() {
            return value;
        }

        /**
         * {@return the name of the field, if it has one}
         */
        @Nonnull
        public Optional<String> name() {
            return name;
        }

        /**
         * {@return the number of the field in the stored record, if it has one}
         */
        @Nonnull
        public Optional<Integer> number() {
            return number;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            return other instanceof Field that && value.equals(that.value) && name.equals(that.name)
                    && number.equals(that.number);
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        private int computeHashCode() {
            return Objects.hash("record constructor field", value, name, number);
        }

        @Nonnull
        @Override
        public String toString() {
            return "Field[value=" + value + ", name=" + name + ", number=" + number + "]";
        }
    }
}
