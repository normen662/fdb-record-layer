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

import com.apple.foundationdb.record.ir.fragments.IExternType;
import com.apple.foundationdb.record.ir.fragments.IFunction;
import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.apple.foundationdb.record.ir.fragments.IPackageStatement;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.fragments.ITypeDeclaration;
import com.google.common.base.Preconditions;
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
 * What is in effect at a point of an FRL-IR program: the type parameters, the extern types, the variables, and the
 * functions that the enclosing constructs declare, the package of the program, the inference that types the
 * expressions there, and the element type of the stream that a yield there adds to. Scopes form a stack. Each scope
 * refers to the scope that encloses it, a lookup searches from the innermost scope outward, so an inner declaration
 * hides an outer one of the same name, and leaving a construct returns to the enclosing scope, which drops what the
 * construct declared.
 *
 * <p>Type parameters and extern types are types and the others are values, and the two have separate namespaces.
 * Within each namespace, a scope declares a name only once, so there is no overloading. A type parameter must not hide
 * a type parameter of an enclosing function: type parameters compare by name, so the two would be the same type. An
 * extern type refers to its declaration, so it can hide another type without that ambiguity.</p>
 *
 * <p>The inference of a scope types the expressions in it, see {@code TypeInference}. A statement outside closures and
 * stream blocks has a scope of its own with a new inference. The scopes of the closures and stream blocks in the
 * statement inherit that inference, so that their statements join it. The scope of a function has no inference, so that
 * the statements of its body each start their own, also inside a closure. A variable belongs to the inference of the
 * scope that declares it, since its type is a term of that inference.</p>
 *
 * <p>The scope of a stream block holds the element type of its stream, the type of every value that a yield in the
 * block adds, and the scopes of the blocks in it inherit the element type. The scopes of functions and closures have
 * none, also within a stream block: a function or a closure can be called when no stream block is running, so a yield
 * in it would add to no stream.</p>
 *
 * <p>A scope holds the type parameters themselves rather than their names, so that a reference to a type parameter
 * resolves to the type that the enclosing function declares. Likewise, a function and an extern type are held by the
 * fragment that defines or declares them, so that a reference resolves to that fragment once, where it is parsed.</p>
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
     * The element type of the stream of the stream block that this scope is in, which a yield here adds to, or
     * {@code null} if this scope is in no stream block, or in a function or a closure within one.
     */
    @Nullable
    private final TypeInference.Term yieldElementType;

    /**
     * The extern types declared in this scope, by name. Like {@link #values}, they are declared while the statements of
     * the scope are inferred.
     */
    @Nonnull
    private final Map<String, ITypeDeclaration> externTypes = new LinkedHashMap<>();

    /**
     * The variables and functions declared in this scope, by name. They are declared while the statements of the scope
     * are inferred, so, like the extern types and the package, they are mutable parts of a scope.
     */
    @Nonnull
    private final Map<String, Binding> values = new LinkedHashMap<>();

    /**
     * The package that the program belongs to, held by the outermost scope, or {@code null} if the program has not
     * named one. Its first statement does that, so the package is known before anything is declared in it.
     */
    @Nullable
    private IPackageStatement packageStatement;

    private Scope(@Nullable final Scope enclosing, @Nonnull final ImmutableMap<String, IGenericType> typeParameters,
                  @Nullable final TypeInference inference, final boolean isStatement,
                  @Nullable final TypeInference.Term yieldElementType) {
        this.enclosing = enclosing;
        this.typeParameters = typeParameters;
        this.inference = inference;
        this.isStatement = isStatement;
        this.yieldElementType = yieldElementType;
    }

    /**
     * Returns the outermost scope, the scope of the statements of a program.
     *
     * @return a new scope that is enclosed by no other scope
     */
    @Nonnull
    static Scope root() {
        return new Scope(null, ImmutableMap.of(), null, false, null);
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
        return new Scope(this, Maps.uniqueIndex(newTypeParameters, IGenericType::name), null, false, null);
    }

    /**
     * Returns the scope of a block nested in this one, the body of a function or a closure, which has the inference of
     * this scope and its element type of yields.
     *
     * @return a new scope enclosed by this one
     */
    @Nonnull
    Scope enterBlock() {
        return new Scope(this, ImmutableMap.of(), inference, false, yieldElementType);
    }

    /**
     * Returns the scope of a closure nested in this one, which has the inference of this scope. The scope has no
     * element type of yields: the closure can be called when no stream block is running, so it cannot yield, even
     * within a stream block.
     *
     * @return a new scope enclosed by this one
     */
    @Nonnull
    Scope enterClosure() {
        return new Scope(this, ImmutableMap.of(), inference, false, null);
    }

    /**
     * Returns the scope of the block of a stream block nested in this one, which has the inference of this scope, so
     * that the statements of the block join it, as those of a closure do.
     *
     * @param elementType the element type of the stream, which every value that a yield in the block adds must have
     * @return a new scope enclosed by this one
     */
    @Nonnull
    Scope enterStream(@Nonnull final TypeInference.Term elementType) {
        return new Scope(this, ImmutableMap.of(), inference, false, elementType);
    }

    /**
     * Returns the scope of a statement outside closures and stream blocks, which types the statement with an inference
     * of its own. The scope declares what the statement declares in this scope.
     *
     * @param statementInference the inference of the statement
     * @return a new scope enclosed by this one
     */
    @Nonnull
    Scope enterStatement(@Nonnull final TypeInference statementInference) {
        Preconditions.checkState(inference == null, "a statement of a closure joins the inference of its closure");
        return new Scope(this, ImmutableMap.of(), statementInference, true, yieldElementType);
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
     * Returns the element type of the stream that a yield in this scope adds to.
     *
     * @return the element type of the stream of the stream block that this scope is in, or {@code null} if this scope
     *         is in no stream block, or in a function or a closure within one
     */
    @Nullable
    TypeInference.Term yieldElementType() {
        return yieldElementType;
    }

    /**
     * Declares the package that the program belongs to. The outermost scope holds it, so that every declaration of the
     * program finds it.
     *
     * @param newPackageStatement the package statement of the program
     */
    void declarePackage(@Nonnull final IPackageStatement newPackageStatement) {
        if (enclosing != null) {
            enclosing.declarePackage(newPackageStatement);
            return;
        }
        Preconditions.checkState(packageStatement == null, "a program belongs to one package at most");
        packageStatement = newPackageStatement;
    }

    /**
     * Returns the package that the program belongs to.
     *
     * @return the package statement of the program, or {@code null} if the program has not named a package
     */
    @Nullable
    IPackageStatement packageStatement() {
        return enclosing == null ? packageStatement : enclosing.packageStatement();
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
     * Declares a function in this scope. A function is declared once its definition is built, so that a reference to it
     * can refer to the definition; its body therefore cannot call it.
     *
     * @param function the definition or declaration of the function, whose name this scope must not declare yet
     */
    void declareFunction(@Nonnull final IFunction function) {
        declare(function.name(), new FunctionBinding(function));
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
     * Declares an extern type in this scope.
     *
     * @param declaration the declaration of the type, whose name this scope must not declare yet as a type
     */
    void declareExternType(@Nonnull final ITypeDeclaration declaration) {
        if (isStatement) {
            // the scope of a statement always has an enclosing scope
            Objects.requireNonNull(enclosing).declareExternType(declaration);
            return;
        }
        Preconditions.checkArgument(!typeParameters.containsKey(declaration.name())
                        && externTypes.putIfAbsent(declaration.name(), declaration) == null,
                "a name can be declared only once in a scope");
    }

    /**
     * Returns the type of the given name: a type parameter or an extern type that is declared in this scope or, if there
     * is none, in the nearest enclosing scope that declares one.
     *
     * @param name the name of the type
     * @return the type, which is not nullable, or empty if no type of this name is in scope
     */
    @Nonnull
    Optional<IType> lookupType(@Nonnull final String name) {
        final IGenericType typeParameter = typeParameters.get(name);
        if (typeParameter != null) {
            return Optional.of(typeParameter);
        }
        final ITypeDeclaration externType = externTypes.get(name);
        if (externType != null) {
            return Optional.of(new IExternType(externType, false));
        }
        return enclosing == null ? Optional.empty() : enclosing.lookupType(name);
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
     * Returns the function of the given name that is declared in this scope or, if there is none, in the nearest
     * enclosing scope that declares one. Unlike {@link #lookupValue}, the lookup skips variables of the name: a dot call,
     * {@code x.f(1)}, calls a function, so a variable {@code f} must not hide the function there.
     *
     * @param name the name of the function
     * @return the function, or empty if no function of this name is in scope
     */
    @Nonnull
    Optional<FunctionBinding> lookupFunction(@Nonnull final String name) {
        if (values.get(name) instanceof FunctionBinding function) {
            return Optional.of(function);
        }
        return enclosing == null ? Optional.empty() : enclosing.lookupFunction(name);
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
     * A function, held by its definition or its declaration, which a reference to the function refers to.
     *
     * @param function the definition or declaration of the function
     */
    record FunctionBinding(@Nonnull IFunction function) implements Binding {
        /**
         * Returns the type parameters of the function, which a reference to it instantiates.
         *
         * @return the type parameters in order
         */
        @Nonnull
        List<IGenericType> typeParameters() {
            return function.typeParameters();
        }

        /**
         * Returns the type of the function, in terms of its type parameters.
         *
         * @return the function type
         */
        @Nonnull
        IFunctionType type() {
            return function.type();
        }
    }
}
