/*
 * IVariableReference.java
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
 * A reference to a variable: the variable of an assignment, or a parameter of a function definition or a closure. A
 * reference names the nearest variable of that name in scope.
 */
@API(API.Status.EXPERIMENTAL)
public final class IVariableReference implements IExpression {
    @Nonnull
    private final String name;
    @Nonnull
    private final IType type;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a reference to a variable.
     *
     * @param name the name of the variable
     * @param type the type of the variable
     */
    public IVariableReference(@Nonnull final String name, @Nonnull final IType type) {
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
    }

    /**
     * {@return the name of the variable}
     */
    @Nonnull
    public String name() {
        return name;
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
        return other instanceof IVariableReference that && name.equals(that.name) && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("variable reference", name, type);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IVariableReference[name=" + name + ", type=" + type + "]";
    }
}
