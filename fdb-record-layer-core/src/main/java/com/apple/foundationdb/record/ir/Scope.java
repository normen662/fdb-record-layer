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

import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * What is in effect at a point of an FRL-IR program: the type parameters, the variables, and the functions that the
 * enclosing constructs declare, and the inference that types the expressions there. Scopes form a stack. Each scope
 * refers to the scope that encloses it, a lookup searches from the innermost scope outward, so an inner declaration
 * hides an outer one of the same name, and leaving a construct returns to the enclosing scope, which drops what the
 * construct declared.
 *
 * <p>Type parameters are types and the others are values, and the two have separate namespaces. Variables and
 * functions share one: a scope declares a name only once, so there is no overloading. A type parameter must not hide a
 * type parameter of an enclosing function: type parameters compare by name, so the two would be the same type.</p>
 *
 * <p>The inference of a scope types the expressions in it, see {@code TypeInference}. A statement outside closures has
 * a scope of its own with a new inference. The scopes of the closures in the statement inherit that inference, so that
 * the closures' statements join it. The scope of a function has no inference, so that the statements of its body each
 * start their own, also inside a closure. A variable belongs to the inference of the scope that declares it, since
 * its type is a term of that inference.</p>
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
     * The inference that types the expressions in this scope, or {@code null} where no statement is being inferred: in
     * the scope of a program, of a function, and of a block of a function.
     */
    @Nullable
    private final TypeInference inference;

    /**
     * Whether this is the scope of a statement. What a statement declares is visible after the statement, so the scope
     * of a statement declares into the enclosing scope.
     */
    private final boolean isStatement;

    /**
     * The variables and functions declared in this scope, by name. They are declared while the statements of the scope
     * are inferred, so this is the one mutable part of a scope.
     */
    @Nonnull
    private final Map<String, Binding> values = new LinkedHashMap<>();

    private Scope(@Nullable final Scope enclosing, @Nonnull final ImmutableMap<String, IGenericType> typeParameters,
                  @Nullable final TypeInference inference, final boolean isStatement) {
        this.enclosing = enclosing;
        this.typeParameters = typeParameters;
        this.inference = inference;
        this.isStatement = isStatement;
    }

    /**
     * Returns the outermost scope, the scope of the statements of a program.
     *
     * @return a new scope that is enclosed by no other scope
     */
    @Nonnull
    static Scope root() {
        return new Scope(null, ImmutableMap.of(), null, false);
    }

    /**
     * Returns the scope of a function definition or declaration nested in this one. It declares the type parameters of
     * the function and has no inference, so the statements of the function's body are each typed on their own.
     *
     * @param newTypeParameters the type parameters of the function; their names must be distinct, and no type parameter
     *        of this scope or an enclosing one may have one of them
     * @return a new scope enclosed by this one
     */
    @Nonnull
    Scope enterFunction(@Nonnull final Collection<IGenericType> newTypeParameters) {
        Preconditions.checkArgument(
                newTypeParameters.stream().map(IGenericType::name).distinct().count() == newTypeParameters.size(),
                "duplicate type parameter");
        Preconditions.checkArgument(
                newTypeParameters.stream().noneMatch(typeParameter -> lookupTypeParameter(typeParameter.name()).isPresent()),
                "a type parameter must not have the name of a type parameter of an enclosing function");
        return new Scope(this, Maps.uniqueIndex(newTypeParameters, IGenericType::name), null, false);
    }

    /**
     * Returns the scope of a block or a closure nested in this one, which has the inference of this scope.
     *
     * @return a new scope enclosed by this one
     */
    @Nonnull
    Scope enterBlock() {
        return new Scope(this, ImmutableMap.of(), inference, false);
    }

    /**
     * Returns the scope of a statement outside closures, which types the statement with an inference of its own. The
     * scope declares what the statement declares in this scope.
     *
     * @param statementInference the inference of the statement
     * @return a new scope enclosed by this one
     */
    @Nonnull
    Scope enterStatement(@Nonnull final TypeInference statementInference) {
        Preconditions.checkState(inference == null, "a statement of a closure joins the inference of its closure");
        return new Scope(this, ImmutableMap.of(), statementInference, true);
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
     * Returns the inference that types the expressions in this scope.
     *
     * @return the inference, or {@code null} if no statement is being inferred here
     */
    @Nullable
    TypeInference inference() {
        return inference;
    }

    /**
     * Declares a variable in this scope. The variable belongs to the inference of this scope.
     *
     * @param name the name of the variable, which this scope must not declare yet
     * @param type the type of the variable, which can contain variables of the inference of this scope
     */
    void declareVariable(@Nonnull final String name, @Nonnull final TypeInference.Term type) {
        declare(name, new VariableBinding(type, inference));
    }

    /**
     * Declares a function in this scope. A function is declared with its signature before its body is built, so that
     * the body can call it.
     *
     * @param name the name of the function, which this scope must not declare yet
     * @param typeParameters the type parameters of the function, in order
     * @param type the type of the function, in terms of its type parameters
     */
    void declareFunction(@Nonnull final String name, @Nonnull final List<IGenericType> typeParameters,
                         @Nonnull final IFunctionType type) {
        declare(name, new FunctionBinding(ImmutableList.copyOf(typeParameters), type));
    }

    private void declare(@Nonnull final String name, @Nonnull final Binding binding) {
        if (isStatement) {
            // the scope of a statement always has an enclosing scope
            Objects.requireNonNull(enclosing).declare(name, binding);
            return;
        }
        Preconditions.checkArgument(values.putIfAbsent(name, binding) == null,
                "a name can be declared only once in a scope");
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
     * Returns the variable or function of the given name that is declared in this scope or, if there is none, in the
     * nearest enclosing scope that declares one.
     *
     * @param name the name of the variable or function
     * @return what the name stands for, or empty if no variable or function of this name is in scope
     */
    @Nonnull
    Optional<Binding> lookupValue(@Nonnull final String name) {
        final Binding binding = values.get(name);
        if (binding != null) {
            return Optional.of(binding);
        }
        return enclosing == null ? Optional.empty() : enclosing.lookupValue(name);
    }

    /**
     * What the name of a value stands for: a variable or a function.
     */
    sealed interface Binding permits VariableBinding, FunctionBinding {
    }

    /**
     * A variable, the variable of an assignment or a parameter, with its type.
     *
     * @param type the type of the variable
     * @param owner the inference of the scope that declares the variable, which the type is a term of, or {@code null}
     *        if the type is known
     */
    record VariableBinding(@Nonnull TypeInference.Term type, @Nullable TypeInference owner) implements Binding {
    }

    /**
     * A function, with what a reference to it needs: its type parameters and its type. The definition is complete only
     * once its body is, and the body can already refer to the function, so a scope does not hold the definition.
     *
     * @param typeParameters the type parameters of the function, in order
     * @param type the type of the function, in terms of its type parameters
     */
    record FunctionBinding(@Nonnull ImmutableList<IGenericType> typeParameters, @Nonnull IFunctionType type)
            implements Binding {
    }
}
