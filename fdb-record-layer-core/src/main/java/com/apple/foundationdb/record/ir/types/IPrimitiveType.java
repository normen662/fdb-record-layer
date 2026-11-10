/*
 * IPrimitiveType.java
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
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A primitive type such as {@code bigint} or {@code string?}.
 */
@API(API.Status.EXPERIMENTAL)
public final class IPrimitiveType implements IType {
    @Nonnull
    private final Kind kind;
    private final boolean isNullable;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a primitive type.
     *
     * @param kind which primitive type this is
     * @param isNullable whether values of this type can be null
     */
    public IPrimitiveType(@Nonnull final Kind kind, final boolean isNullable) {
        this.kind = Objects.requireNonNull(kind);
        this.isNullable = isNullable;
    }

    /**
     * {@return which primitive type this is}
     */
    @Nonnull
    public Kind kind() {
        return kind;
    }

    @Override
    public boolean isNullable() {
        return isNullable;
    }

    @Nonnull
    @Override
    public IPrimitiveType withNullability(final boolean isNullable) {
        return new IPrimitiveType(kind, isNullable);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IPrimitiveType that && kind == that.kind && isNullable == that.isNullable;
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        // the keyword and not the enum constant: the hash code of an enum constant is its identity hash code
        return Objects.hash("primitive type", kind.getKeyword(), isNullable);
    }

    @Nonnull
    @Override
    public String toString() {
        return kind.getKeyword() + ParserHelpers.nullableSuffix(isNullable);
    }

    /**
     * The primitive types of FRL-IR. {@link #HALF}, {@link #FLOAT}, and {@link #DOUBLE} are the 16-bit, 32-bit, and
     * 64-bit floating-point types. {@link #VOID} is the unit type, the result type of a function that returns nothing.
     */
    public enum Kind {
        BOOLEAN("boolean"),
        INT("int"),
        BIGINT("bigint"),
        HALF("half"),
        FLOAT("float"),
        DOUBLE("double"),
        STRING("string"),
        BYTES("bytes"),
        UUID("uuid"),
        VERSION("version"),
        VOID("void");

        @Nonnull
        private final String keyword;

        Kind(@Nonnull final String keyword) {
            this.keyword = keyword;
        }

        /**
         * Returns the FRL-IR keyword that names this type.
         *
         * @return the keyword, e.g. {@code bigint}
         */
        @Nonnull
        public String getKeyword() {
            return keyword;
        }
    }
}
