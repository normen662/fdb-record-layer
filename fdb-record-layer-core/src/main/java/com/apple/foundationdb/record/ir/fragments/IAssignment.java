/*
 * IAssignment.java
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

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * An assignment, {@code let x = f(1)}: a statement that names the value of an expression. The variable is immutable,
 * and its type is the type of the expression. A type stated in the text, {@code let x: bigint = 42}, only guides the
 * inference of the expression's type, which it then equals, so the assignment does not keep it. An expression of type
 * {@code void} has no value to name.
 */
@API(API.Status.EXPERIMENTAL)
public final class IAssignment implements IStatement {
    @Nonnull
    private final String name;
    @Nonnull
    private final IExpression expression;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates an assignment.
     *
     * @param name the name of the variable
     * @param expression the expression whose value the variable names, which cannot be of type {@code void}
     */
    public IAssignment(@Nonnull final String name, @Nonnull final IExpression expression) {
        this.name = Objects.requireNonNull(name);
        this.expression = Objects.requireNonNull(expression);
        Preconditions.checkArgument(!expression.type().isVoid(), "void is only a result type");
    }

    /**
     * {@return the name of the variable}
     */
    @Nonnull
    public String name() {
        return name;
    }

    /**
     * {@return the expression whose value the variable names; its type is the type of the variable}
     */
    @Nonnull
    public IExpression expression() {
        return expression;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IAssignment that && name.equals(that.name) && expression.equals(that.expression);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("assignment", name, expression);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IAssignment[name=" + name + ", expression=" + expression + "]";
    }
}
