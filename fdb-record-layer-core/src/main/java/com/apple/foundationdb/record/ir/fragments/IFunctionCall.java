/*
 * IFunctionCall.java
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
import java.util.function.Supplier;

/**
 * A call, {@code f(1, x)}: an expression of function type, the callee, applied to arguments. The callee can be any
 * expression of a function type that is not nullable, so {@code f(1)(2)} calls the result of a call. The type of a call
 * is the result type of its callee. Like every expression, a call is also a statement: {@code f(1)} on its own.
 *
 * <p>A call is well-typed by construction: the type of every argument is the type of its parameter. There is no
 * promotion, so an argument of type {@code int} cannot be passed for a parameter of type {@code int?}.</p>
 */
@API(API.Status.EXPERIMENTAL)
public final class IFunctionCall implements IExpression {
    @Nonnull
    private final IExpression callee;
    @Nonnull
    private final ImmutableList<IExpression> arguments;
    @Nonnull
    private final IType type;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates a call.
     *
     * @param callee the expression to call, of a function type that is not nullable
     * @param arguments the arguments in order, each of the type of its parameter
     */
    public IFunctionCall(@Nonnull final IExpression callee, @Nonnull final List<IExpression> arguments) {
        this.callee = Objects.requireNonNull(callee);
        this.arguments = ImmutableList.copyOf(arguments);
        if (!(callee.type() instanceof IFunctionType functionType) || functionType.isNullable()) {
            throw new IllegalArgumentException("only a function that is not nullable can be called");
        }
        Preconditions.checkArgument(functionType.parameterTypes()
                        .equals(this.arguments.stream().map(IExpression::type).collect(ImmutableList.toImmutableList())),
                "the types of the arguments of a call must be the types of the parameters of the function");
        this.type = functionType.resultType();
    }

    /**
     * {@return the expression that is called}
     */
    @Nonnull
    public IExpression callee() {
        return callee;
    }

    /**
     * {@return the arguments in order}
     */
    @Nonnull
    public List<IExpression> arguments() {
        return arguments;
    }

    /**
     * Returns the type of the call, the result type of the callee.
     *
     * @return the result type of the function that is called
     */
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
        return other instanceof IFunctionCall that && callee.equals(that.callee) && arguments.equals(that.arguments);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("function call", callee, arguments);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IFunctionCall[callee=" + callee + ", arguments=" + arguments + "]";
    }
}
