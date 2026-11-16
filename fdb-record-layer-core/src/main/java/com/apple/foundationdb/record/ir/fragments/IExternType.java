/*
 * IExternType.java
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

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * An extern type, {@code range} after {@code extern type range}: a type that the runtime provides. Its values come from
 * and go to extern functions; FRL-IR only passes them along. The type refers back to its
 * {@link ITypeDeclarationFragment}, so that a target can tell from the declaration what the type is outside FRL-IR.
 * Equality compares the declaration and the nullability.
 */
@API(API.Status.EXPERIMENTAL)
public final class IExternType implements IType {
    @Nonnull
    private final ITypeDeclarationFragment declaration;
    private final boolean isNullable;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates an extern type.
     *
     * @param declaration the declaration of the type
     * @param isNullable whether values of this type can be null
     */
    public IExternType(@Nonnull final ITypeDeclarationFragment declaration, final boolean isNullable) {
        this.declaration = Objects.requireNonNull(declaration);
        this.isNullable = isNullable;
    }

    /**
     * {@return the declaration of the type}
     */
    @Nonnull
    public ITypeDeclarationFragment declaration() {
        return declaration;
    }

    /**
     * {@return the name of the type}
     */
    @Nonnull
    public String name() {
        return declaration.name();
    }

    @Override
    public boolean isNullable() {
        return isNullable;
    }

    @Nonnull
    @Override
    public IExternType withNullability(final boolean isNullable) {
        return new IExternType(declaration, isNullable);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IExternType that && declaration.equals(that.declaration)
                && isNullable == that.isNullable;
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("extern type", declaration, isNullable);
    }

    @Nonnull
    @Override
    public String toString() {
        return ParserHelpers.writeName(name()) + ParserHelpers.nullableSuffix(isNullable);
    }
}
