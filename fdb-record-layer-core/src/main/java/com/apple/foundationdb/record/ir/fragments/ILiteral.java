/*
 * ILiteral.java
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
import com.apple.foundationdb.half.Half;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A literal: {@code 42}, {@code 1.5}, {@code "text"}, {@code true}, or {@code nil}. A literal takes its type from its
 * context, so the same spelling denotes values of different types: {@code 42} is an int on its own and a bigint where a
 * bigint is required. A literal therefore holds its value as a Java object of the type it has, a bigint {@code 42} as a
 * {@link Long}.
 */
@API(API.Status.EXPERIMENTAL)
public final class ILiteral implements IExpression {
    @Nullable
    private final Object value;
    @Nonnull
    private final IType type;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a literal.
     *
     * @param value the value, or {@code null} for {@code nil}: a {@link Boolean} for a boolean, an {@link Integer} for
     *        an int, a {@link Long} for a bigint, a {@link Half} for a half, a {@link Float} for a float, a
     *        {@link Double} for a double, and a {@link String} for a string
     * @param type the type of the literal: one of these types, or its nullable version; {@code nil} can be of any
     *        nullable type
     */
    public ILiteral(@Nullable final Object value, @Nonnull final IType type) {
        this.value = value;
        this.type = Objects.requireNonNull(type);
        Preconditions.checkArgument(value == null ? type.isNullable() : isValueOf(value, type),
                "the value of a literal must be of the type of the literal");
    }

    private static boolean isValueOf(@Nonnull final Object value, @Nonnull final IType type) {
        if (!(type instanceof IPrimitiveType primitiveType)) {
            return false;
        }
        return switch (primitiveType.kind()) {
            case BOOLEAN -> value instanceof Boolean;
            case INT -> value instanceof Integer;
            case BIGINT -> value instanceof Long;
            case HALF -> value instanceof Half;
            case FLOAT -> value instanceof Float;
            case DOUBLE -> value instanceof Double;
            case STRING -> value instanceof String;
            default -> false;
        };
    }

    /**
     * {@return the value, or {@code null} if the literal is {@code nil}}
     */
    @Nullable
    public Object value() {
        return value;
    }

    @Nonnull
    @Override
    public IType type() {
        return type;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ILiteral that && Objects.equals(value, that.value) && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("literal", value, type);
    }

    @Nonnull
    @Override
    public String toString() {
        return "ILiteral[value=" + value + ", type=" + type + "]";
    }
}
