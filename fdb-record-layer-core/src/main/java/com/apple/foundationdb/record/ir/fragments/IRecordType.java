/*
 * IRecordType.java
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
import java.util.stream.Collectors;

/**
 * A record type, generic over any number of field types: {@code record<id(1): bigint, name: string?, int>}. Records
 * have no name of their own.
 */
@API(API.Status.EXPERIMENTAL)
public final class IRecordType implements IType {
    private final boolean isNullable;
    @Nonnull
    private final ImmutableList<Field> fields;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a record type.
     *
     * @param isNullable whether values of this type can be null
     * @param fields the fields in order
     */
    public IRecordType(final boolean isNullable, @Nonnull final List<Field> fields) {
        this.isNullable = isNullable;
        this.fields = ImmutableList.copyOf(fields);
    }

    @Override
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

    @Nonnull
    @Override
    public IRecordType withNullability(final boolean isNullable) {
        return new IRecordType(isNullable, fields);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IRecordType that && isNullable == that.isNullable && fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("record type", isNullable, fields);
    }

    @Nonnull
    @Override
    public String toString() {
        return fields.stream().map(Field::toString).collect(Collectors.joining(", ", "record<", ">"))
                + ParserHelpers.nullableSuffix(isNullable);
    }

    /**
     * A field of a record type. The number is the field's number in the stored record. Only a named field can state a
     * number, because the syntax attaches the number to the name: {@code id(1): bigint}. A name that is a keyword or
     * not an identifier is written between backticks, as in Swift: {@code `version`(2): bigint}.
     */
    public static final class Field implements IFragment {
        @Nonnull
        private final IType type;
        @Nonnull
        private final Optional<String> name;
        @Nonnull
        private final Optional<Integer> number;
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        /**
         * Creates a field.
         *
         * @param type the type of the field, which cannot be {@code void}
         * @param name the name of the field, if it has one
         * @param number the number of the field in the stored record, if it is known; only a named field can have one
         */
        public Field(@Nonnull final IType type, @Nonnull final Optional<String> name,
                     @Nonnull final Optional<Integer> number) {
            this.type = Objects.requireNonNull(type);
            this.name = Objects.requireNonNull(name);
            this.number = Objects.requireNonNull(number);
            name.ifPresent(ParserHelpers::checkWritableName);
            Preconditions.checkArgument(name.isPresent() || number.isEmpty(), "a field number requires a field name");
            Preconditions.checkArgument(!type.isVoid(), "void is only a result type");
        }

        /**
         * Creates a field without name and number.
         *
         * @param type the type of the field
         * @return a new field
         */
        @Nonnull
        public static Field unnamedOf(@Nonnull final IType type) {
            return new Field(type, Optional.empty(), Optional.empty());
        }

        /**
         * Creates a named field without number.
         *
         * @param type the type of the field
         * @param name the name of the field
         * @return a new field
         */
        @Nonnull
        public static Field of(@Nonnull final IType type, @Nonnull final String name) {
            return new Field(type, Optional.of(name), Optional.empty());
        }

        /**
         * Creates a named field with a number.
         *
         * @param type the type of the field
         * @param name the name of the field
         * @param number the number of the field in the stored record
         * @return a new field
         */
        @Nonnull
        public static Field of(@Nonnull final IType type, @Nonnull final String name, final int number) {
            return new Field(type, Optional.of(name), Optional.of(number));
        }

        /**
         * {@return the type of the field}
         */
        @Nonnull
        public IType type() {
            return type;
        }

        /**
         * {@return the name of the field, if it has one}
         */
        @Nonnull
        public Optional<String> name() {
            return name;
        }

        /**
         * {@return the number of the field in the stored record, if it is known}
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
            return other instanceof Field that && type.equals(that.type) && name.equals(that.name)
                    && number.equals(that.number);
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        private int computeHashCode() {
            return Objects.hash("record type field", type, name, number);
        }

        @Nonnull
        @Override
        public String toString() {
            return name.map(fieldName -> ParserHelpers.writeName(fieldName)
                            + number.map(fieldNumber -> "(" + fieldNumber + ")").orElse("") + ": ")
                    .orElse("") + type;
        }
    }
}
