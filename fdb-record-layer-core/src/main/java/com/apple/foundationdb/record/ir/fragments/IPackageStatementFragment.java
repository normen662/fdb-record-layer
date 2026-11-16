/*
 * IPackageStatementFragment.java
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
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The package statement of a program, {@code @java("com.apple.foundationdb.record.ir") package abc}: the name of the
 * package that the program and everything it declares belong to. It can only be the first statement of a program. Its
 * annotations say where the package lives outside FRL-IR, which locates the package's extern types and functions there
 * unless their own annotations say otherwise.
 */
@API(API.Status.EXPERIMENTAL)
public final class IPackageStatementFragment implements IStatementFragment {
    @Nonnull
    private final String name;
    @Nonnull
    private final ImmutableList<IAnnotationFragment> annotations;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a package statement.
     *
     * @param name the name of the package
     * @param annotations the annotations of the package, in order
     */
    public IPackageStatementFragment(@Nonnull final String name, @Nonnull final List<IAnnotationFragment> annotations) {
        this.name = Objects.requireNonNull(name);
        this.annotations = ImmutableList.copyOf(annotations);
        ParserHelpers.checkWritableName(name);
    }

    /**
     * {@return the name of the package}
     */
    @Nonnull
    public String name() {
        return name;
    }

    /**
     * {@return the annotations of the package, in order}
     */
    @Nonnull
    public List<IAnnotationFragment> annotations() {
        return annotations;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IPackageStatementFragment that && name.equals(that.name)
                && annotations.equals(that.annotations);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("package statement", name, annotations);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IPackageStatementFragment[name=" + name + ", annotations=" + annotations + "]";
    }
}
