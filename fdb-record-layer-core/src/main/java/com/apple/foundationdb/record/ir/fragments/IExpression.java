/*
 * IExpression.java
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

import javax.annotation.Nonnull;

/**
 * An expression: a literal, a reference to a variable or a function, a call, or a closure. Every expression has a type,
 * and the fragments are typed when they are built: the type of a literal is the one inferred from its context, and the
 * type of a reference to a generic function is the one for its inferred type arguments. Every expression is also a
 * statement.
 */
@API(API.Status.EXPERIMENTAL)
public sealed interface IExpression extends IStatement
        permits ILiteral, IVariableReference, IFunctionReference, IFunctionCall, IClosure {

    /**
     * Returns the type of the value of this expression.
     *
     * @return the type of this expression
     */
    @Nonnull
    IType type();
}
