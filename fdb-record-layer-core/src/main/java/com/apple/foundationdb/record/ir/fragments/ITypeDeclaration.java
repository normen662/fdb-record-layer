/*
 * ITypeDeclaration.java
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
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A type declaration, {@code extern type range}: a type that the runtime provides, for the values that extern functions
 * take and return. FRL-IR knows nothing about such a type but its name, and the type has no type parameters. Every use
 * of the type, an {@link IExternType}, refers back to this declaration, so that a target can find out from the
 * annotations, and from those of the {@linkplain #packageStatement() package}, what the type is outside FRL-IR.
 */
@API(API.Status.EXPERIMENTAL)
public final class ITypeDeclaration implements IStatement {
    @Nonnull
    private final String name;
    @Nonnull
    private final ImmutableList<IAnnotationFragment> annotations;
    @Nullable
    private final IPackageStatement packageStatement;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a type declaration.
     *
     * @param name the name of the type
     * @param annotations the annotations of the declaration, in order
     * @param packageStatement the package of the program that declares the type, or {@code null} if the program has
     *        none
     */
    public ITypeDeclaration(@Nonnull final String name, @Nonnull final List<IAnnotationFragment> annotations,
                            @Nullable final IPackageStatement packageStatement) {
        this.name = Objects.requireNonNull(name);
        this.annotations = ImmutableList.copyOf(annotations);
        this.packageStatement = packageStatement;
        ParserHelpers.checkWritableName(name);
    }

    /**
     * {@return the name of the type}
     */
    @Nonnull
    public String name() {
        return name;
    }

    /**
     * {@return the annotations of the declaration, in order}
     */
    @Nonnull
    public List<IAnnotationFragment> annotations() {
        return annotations;
    }

    /**
     * {@return the package of the program that declares the type, or {@code null} if the program has none}
     */
    @Nullable
    public IPackageStatement packageStatement() {
        return packageStatement;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ITypeDeclaration that && name.equals(that.name)
                && annotations.equals(that.annotations) && Objects.equals(packageStatement, that.packageStatement);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("type declaration", name, annotations, packageStatement);
    }

    @Nonnull
    @Override
    public String toString() {
        return "ITypeDeclaration[name=" + name + ", annotations=" + annotations + ", packageStatement="
                + packageStatement + "]";
    }
}
