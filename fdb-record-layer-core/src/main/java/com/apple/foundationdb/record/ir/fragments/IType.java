/*
 * IType.java
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
 * A type of FRL-IR. FRL-IR is strictly typed: there is no type promotion and there are no subtypes, so two types are
 * interchangeable exactly when they are equal.
 *
 * <p>Types are immutable values. {@link Object#toString() toString()} renders a type in FRL-IR syntax, and parsing that
 * text yields an equal type, provided the type parameters it mentions ({@link IGenericType}) are in scope.</p>
 */
@API(API.Status.EXPERIMENTAL)
public sealed interface IType extends IFragment permits IPrimitiveType, IRecordType, IArrayType, IVectorType, IEnumType,
        IFunctionType, IStreamType, IGenericType {

    /**
     * Returns whether values of this type can be null.
     *
     * @return {@code true} if values of this type can be null
     */
    boolean isNullable();

    /**
     * Returns this type with the given nullability.
     *
     * @param isNullable whether values of the returned type can be null
     * @return a type that differs from this one at most in its nullability
     */
    @Nonnull
    IType withNullability(boolean isNullable);

    /**
     * Returns whether this type is {@code void}, the result type of a function or a closure that produces no value.
     * {@code void} is a type only in that place: the fragments reject it as the type of a parameter, a field, an
     * element, or a variable, and they reject {@code void?}.
     *
     * @return {@code true} if this type is {@code void}
     */
    default boolean isVoid() {
        return false;
    }
}
