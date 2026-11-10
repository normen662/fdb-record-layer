/*
 * Scope.java
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

package com.apple.foundationdb.record.ir;

import com.apple.foundationdb.record.ir.types.IGenericType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/**
 * What is declared at a point of an FRL-IR program: the type parameters and the functions of the constructs that enclose
 * it. Scopes form a stack. Each scope refers to the scope that encloses it, a lookup searches from the innermost scope
 * outward, so an inner declaration hides an outer one of the same name, and leaving a construct returns to the
 * enclosing scope, which drops what the construct declared.
 *
 * <p>A scope holds the type parameters themselves rather than their names, so that a reference to a type parameter
 * resolves to the type that the enclosing function declares.</p>
 */
final class Scope {

    @Nullable
    private final Scope enclosing;

    /**
     * The type parameters declared in this scope, by name.
     */
    @Nonnull
    private final ImmutableMap<String, IGenericType> typeParameters;

    /**
     * The names of the functions declared in this scope. A function's name has to be in scope before the function's
     * definition is complete, so that its own body can refer to it, which is why the scope holds names and not
     * definitions. The functions are added while the statements of the scope are built, so this is the one mutable part
     * of a scope.
     */
    @Nonnull
    private final Set<String> functions = new LinkedHashSet<>();

    private Scope(@Nullable final Scope enclosing, @Nonnull final ImmutableMap<String, IGenericType> typeParameters) {
        this.enclosing = enclosing;
        this.typeParameters = typeParameters;
    }

    /**
     * Returns the outermost scope, the scope of the statements of a program.
     *
     * @return a new scope that is enclosed by no other scope
     */
    @Nonnull
    static Scope root() {
        return new Scope(null, ImmutableMap.of());
    }

    /**
     * Returns a scope nested in this one, for a construct that declares the given type parameters.
     *
     * @param newTypeParameters the type parameters that the construct declares; their names must be distinct
     * @return a new scope enclosed by this one
     */
    @Nonnull
    Scope enter(@Nonnull final Collection<IGenericType> newTypeParameters) {
        Preconditions.checkArgument(
                newTypeParameters.stream().map(IGenericType::name).distinct().count() == newTypeParameters.size(),
                "duplicate type parameter");
        return new Scope(this, Maps.uniqueIndex(newTypeParameters, IGenericType::name));
    }

    /**
     * Returns the scope that encloses this one, the scope in effect after the construct of this scope.
     *
     * @return the enclosing scope
     */
    @Nonnull
    Scope exit() {
        if (enclosing == null) {
            throw new IllegalStateException("the outermost scope has no enclosing scope");
        }
        return enclosing;
    }

    /**
     * Declares a function in this scope.
     *
     * @param name the name of the function
     */
    void declareFunction(@Nonnull final String name) {
        functions.add(name);
    }

    /**
     * Returns the type parameter of the given name that is declared in this scope or, if there is none, in the nearest
     * enclosing scope that declares one.
     *
     * @param name the name of the type parameter
     * @return the type parameter, or empty if no type parameter of this name is in scope
     */
    @Nonnull
    Optional<IGenericType> lookupTypeParameter(@Nonnull final String name) {
        final IGenericType typeParameter = typeParameters.get(name);
        if (typeParameter != null) {
            return Optional.of(typeParameter);
        }
        return enclosing == null ? Optional.empty() : enclosing.lookupTypeParameter(name);
    }

    /**
     * Returns whether a name is a function here, i.e. one declared in this scope or in an enclosing one.
     *
     * @param name the name
     * @return {@code true} if the name is a function in scope
     */
    boolean isFunction(@Nonnull final String name) {
        return functions.contains(name) || (enclosing != null && enclosing.isFunction(name));
    }
}
