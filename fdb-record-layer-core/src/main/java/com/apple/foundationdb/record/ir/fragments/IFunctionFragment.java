/*
 * IFunctionFragment.java
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
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * A function that a name can stand for: one that FRL-IR defines, an {@link IFunctionDefinitionFragment}, or one that
 * the runtime provides, an {@link IFunctionDeclarationFragment}. A reference to a function, an
 * {@link IFunctionReferenceFragment}, refers back to it, so that lowering finds the definition to compile, or the
 * declaration that says what to call. Either kind can be a variant of an {@link IOverloadedFunctionFragment}, see
 * {@link #variant()}.
 */
@API(API.Status.EXPERIMENTAL)
public sealed interface IFunctionFragment extends IStatementFragment
        permits IFunctionDefinitionFragment, IFunctionDeclarationFragment {

    /**
     * Returns the name of the function.
     *
     * @return the name
     */
    @Nonnull
    String name();

    /**
     * Returns the type parameters of the function.
     *
     * @return the type parameters of a generic function, in order, and an empty list otherwise
     */
    @Nonnull
    List<IGenericType> typeParameters();

    /**
     * Returns the parameters of the function.
     *
     * @return the parameters in order
     */
    @Nonnull
    List<IFunctionDefinitionFragment.Parameter> parameters();

    /**
     * Returns the result type of the function.
     *
     * @return the result type, which is {@code void} if the function does not state one
     */
    @Nonnull
    IType resultType();

    /**
     * Returns the annotations of the function, which say how the function lives outside FRL-IR.
     *
     * @return the annotations in order
     */
    @Nonnull
    List<IAnnotationFragment> annotations();

    /**
     * Returns what makes this function a variant of an overloaded function: the overloaded function and the type
     * arguments that select this variant. A call of the overloaded function that selects these type arguments calls
     * this function.
     *
     * @return the variant part, or empty if this function is not a variant
     */
    @Nonnull
    Optional<IOverloadedFunctionFragment.Variant> variant();

    /**
     * Returns the type of the function, in terms of its type parameters.
     *
     * @return a function type that is not nullable
     */
    @Nonnull
    default IFunctionType type() {
        return new IFunctionType(false, parameters().stream()
                .map(IFunctionDefinitionFragment.Parameter::type)
                .collect(ImmutableList.toImmutableList()), resultType());
    }
}
