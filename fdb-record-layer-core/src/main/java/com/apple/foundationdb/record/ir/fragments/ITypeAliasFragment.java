/*
 * ITypeAliasFragment.java
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
 * A type alias, {@code typealias row = record<id: bigint>}: a name for a type, as in Swift. The alias is not a type of
 * its own: a type that names the alias is parsed as the type that the alias stands for, so no type of a fragment refers
 * to an alias, and a value of type {@code row} is a value of the record type. The alias remains a statement of the
 * block that declares it, as in the text of the program.
 */
@API(API.Status.EXPERIMENTAL)
public final class ITypeAliasFragment implements IStatementFragment {
    @Nonnull
    private final String name;
    @Nonnull
    private final IType type;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a type alias.
     *
     * @param name the name of the alias
     * @param type the type that the alias stands for
     */
    public ITypeAliasFragment(@Nonnull final String name, @Nonnull final IType type) {
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
        ParserHelpers.checkWritableName(name);
    }

    /**
     * {@return the name of the alias}
     */
    @Nonnull
    public String name() {
        return name;
    }

    /**
     * {@return the type that the alias stands for}
     */
    @Nonnull
    public IType type() {
        return type;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ITypeAliasFragment that && name.equals(that.name) && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("type alias", name, type);
    }

    @Nonnull
    @Override
    public String toString() {
        return "ITypeAliasFragment[name=" + name + ", type=" + type + "]";
    }
}
