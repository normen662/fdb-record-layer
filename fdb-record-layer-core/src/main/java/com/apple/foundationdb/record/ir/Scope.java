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
import com.apple.foundationdb.record.ir.fragments.IFunctionFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.apple.foundationdb.record.ir.fragments.IOverloadedFunctionFragment;
import com.apple.foundationdb.record.ir.fragments.IPackageStatementFragment;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.fragments.ITypeAliasFragment;
import com.apple.foundationdb.record.ir.fragments.ITypeDeclarationFragment;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * What is in effect at a point of an FRL-IR program: the type parameters, the extern types, the type aliases, the
 * variables, and the functions that the enclosing constructs declare, the package of the program, the inference that
 * types the expressions there, and the element type of the stream that a yield there adds to. Scopes form a stack.
 * Each scope refers to the scope that encloses it, a lookup searches from the innermost scope outward, so an inner
 * declaration hides an outer one of the same name, and leaving a construct returns to the enclosing scope, which drops
 * what the construct declared.
 *
 * <p>Type parameters, extern types, and type aliases are types and the others are values, and the two have separate
 * namespaces. Within each namespace, a scope declares a name only once. The one exception is an overloaded function:
 * its variants have its name, and they are declared after it in the same scope, see
 * {@link OverloadedFunctionBinding}. A type parameter must not hide a type parameter of an enclosing function: type
 * parameters compare by name, so the two would be the same type. An extern type refers to its declaration, and a type
 * alias stands for its type, so either can hide another type without that ambiguity.</p>
 *
 * <p>Operators have a namespace of their own. An {@code @operator} annotation declares its operator in the scope of
 * the annotated function, a scope declares an operator once at most, and an inner declaration of an operator hides an
 * outer one, as for names.</p>
 *
 * <p>The inference of a scope types the expressions in it, see {@code TypeInference}. A statement outside closures and
 * stream blocks has a scope of its own with a new inference. The scopes of the closures and stream blocks in the
 * statement inherit that inference, so that their statements join it. The scope of a function has no inference, so that
 * the statements of its body each start their own, also inside a closure, and neither have the blocks of an if or a for
 * that starts its own, so that their statements are typed as those around the if or the for. A variable belongs to the
 * inference of the scope that declares it, since its type is a term of that inference.</p>
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
     * the scope of a program, of a function, of a block of a function, and of a block of an if or a for that has an
     * inference of its own.
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
     * The extern types and the type aliases declared in this scope, by name: an extern type as the type of its
     * declaration, and an alias as the type it stands for. Like {@link #values}, they are declared while the statements
     * of the scope are inferred.
     */
    @Nonnull
    private final Map<String, IType> namedTypes = new LinkedHashMap<>();

    /**
     * The variables and functions declared in this scope, by name. They are declared while the statements of the scope
     * are inferred, so, like the named types and the package, they are mutable parts of a scope.
     */
    @Nonnull
    private final Map<String, Binding> values = new LinkedHashMap<>();

    /**
     * The operators declared in this scope, each with the function that it calls. Like {@link #values}, they are
     * declared while the statements of the scope are inferred.
     */
    @Nonnull
    private final Map<Operator, CalleeBinding> operators = new EnumMap<>(Operator.class);

    /**
     * The package that the program belongs to, held by the outermost scope, or {@code null} if the program has not
     * named one. Its first statement does that, so the package is known before anything is declared in it.
     */
    @Nullable
    private IPackageStatementFragment packageStatement;

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
     * Returns the scope of a block nested in this one, which has the inference of this scope and its element type of
     * yields: the body of a function or a closure, or a block of a statement of control flow that joins the inference
     * of this scope, such as an if in a closure.
     *
     * @return a new scope enclosed by this one
     */
    @Nonnull
    Scope enterBlock() {
        return new Scope(this, ImmutableMap.of(), inference, false, yieldElementType);
    }

    /**
     * Returns the scope of a block of an if or a for whose statement is typed with an inference of its own, as a
     * statement of a program or of a function body is. The scope has no inference, so that the statements of the block
     * are typed one by one, as the statements around the if or the for are. It has the element type of yields of this
     * scope, since the block runs as part of the block that the if or the for is in.
     *
     * @return a new scope enclosed by this one
     */
    @Nonnull
    Scope enterBlockWithoutInference() {
        return new Scope(this, ImmutableMap.of(), null, false, yieldElementType);
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
    void declarePackage(@Nonnull final IPackageStatementFragment newPackageStatement) {
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
    IPackageStatementFragment packageStatement() {
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
     * @return the binding of the function
     */
    @Nonnull
    FunctionBinding declareFunction(@Nonnull final IFunctionFragment function) {
        final FunctionBinding binding = new FunctionBinding(function);
        declare(function.name(), binding);
        return binding;
    }

    /**
     * Declares an overloaded function in this scope. Its variants are declared after it, in this scope, see
     * {@link #declareVariant}.
     *
     * @param function the overloaded function, whose name this scope must not declare yet
     * @return the binding of the function, which collects its variants
     */
    @Nonnull
    OverloadedFunctionBinding declareOverloadedFunction(@Nonnull final IOverloadedFunctionFragment function) {
        final OverloadedFunctionBinding binding = new OverloadedFunctionBinding(function);
        declare(function.name(), binding);
        return binding;
    }

    /**
     * Returns the overloaded function of the given name that this scope declares. Only in that scope can a variant of
     * the function be declared. The scope of a statement looks in the scope that encloses it, which its declarations
     * go to.
     *
     * @param name the name of the overloaded function
     * @return the overloaded function, or empty if this scope declares no overloaded function of this name
     */
    @Nonnull
    Optional<OverloadedFunctionBinding> overloadedFunction(@Nonnull final String name) {
        if (isStatement) {
            // the scope of a statement always has an enclosing scope
            return Objects.requireNonNull(enclosing).overloadedFunction(name);
        }
        return values.get(name) instanceof OverloadedFunctionBinding function
                ? Optional.of(function)
                : Optional.empty();
    }

    /**
     * Declares a variant of an overloaded function that this scope declares. A call that follows the variant can then
     * select it.
     *
     * @param variant the variant, which must be a variant of an overloaded function of this scope, with a key that no
     *        other variant of that function has
     */
    void declareVariant(@Nonnull final IFunctionFragment variant) {
        final IOverloadedFunctionFragment overloadedFunction = variant.variant()
                .orElseThrow(() -> new IllegalArgumentException("only a variant can be declared as a variant"))
                .overloadedFunction();
        overloadedFunction(variant.name())
                .filter(function -> function.function().equals(overloadedFunction))
                .orElseThrow(() -> new IllegalArgumentException(
                        "a variant must be declared after its overloaded function, in the same scope"))
                .addVariant(variant);
    }

    /**
     * Declares an operator in this scope, the operator of an {@code @operator} annotation of a function.
     *
     * @param operator the operator, which this scope must not declare yet
     * @param function the function that the operator calls
     */
    void declareOperator(@Nonnull final Operator operator, @Nonnull final CalleeBinding function) {
        if (isStatement) {
            // the scope of a statement always has an enclosing scope
            Objects.requireNonNull(enclosing).declareOperator(operator, function);
            return;
        }
        Preconditions.checkArgument(operators.putIfAbsent(operator, function) == null,
                "an operator can be declared only once in a scope");
    }

    /**
     * Returns the function that an operator calls: the function of the operator that this scope declares or, if there
     * is none, the nearest enclosing scope that declares one.
     *
     * @param operator the operator
     * @return the function that the operator calls, or empty if no function of this operator is in scope
     */
    @Nonnull
    Optional<CalleeBinding> lookupOperator(@Nonnull final Operator operator) {
        final CalleeBinding function = operators.get(operator);
        if (function != null) {
            return Optional.of(function);
        }
        return enclosing == null ? Optional.empty() : enclosing.lookupOperator(operator);
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
    void declareExternType(@Nonnull final ITypeDeclarationFragment declaration) {
        declareNamedType(declaration.name(), new IExternType(declaration, false));
    }

    /**
     * Declares a type alias in this scope, so that a type that names the alias is the type that the alias stands for.
     *
     * @param alias the type alias, whose name this scope must not declare yet as a type
     */
    void declareTypeAlias(@Nonnull final ITypeAliasFragment alias) {
        declareNamedType(alias.name(), alias.type());
    }

    private void declareNamedType(@Nonnull final String name, @Nonnull final IType type) {
        if (isStatement) {
            // the scope of a statement always has an enclosing scope
            Objects.requireNonNull(enclosing).declareNamedType(name, type);
            return;
        }
        Preconditions.checkArgument(!typeParameters.containsKey(name) && namedTypes.putIfAbsent(name, type) == null,
                "a name can be declared only once in a scope");
    }

    /**
     * Returns the type of the given name: a type parameter, an extern type, or the type that a type alias stands for,
     * that is declared in this scope or, if there is none, in the nearest enclosing scope that declares one.
     *
     * @param name the name of the type
     * @return the type, or empty if no type of this name is in scope; a type parameter and an extern type are not
     *         nullable, while the type of an alias can be
     */
    @Nonnull
    Optional<IType> lookupType(@Nonnull final String name) {
        final IGenericType typeParameter = typeParameters.get(name);
        if (typeParameter != null) {
            return Optional.of(typeParameter);
        }
        final IType namedType = namedTypes.get(name);
        if (namedType != null) {
            return Optional.of(namedType);
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
     * @return the function, which can be overloaded, or empty if no function of this name is in scope
     */
    @Nonnull
    Optional<CalleeBinding> lookupFunction(@Nonnull final String name) {
        if (values.get(name) instanceof CalleeBinding function) {
            return Optional.of(function);
        }
        return enclosing == null ? Optional.empty() : enclosing.lookupFunction(name);
    }

    /**
     * What the name of a value stands for: a variable or a function, which can be overloaded.
     */
    sealed interface Binding permits VariableBinding, CalleeBinding {
    }

    /**
     * What a call can name: a function, or an overloaded function, whose call calls one of its variants.
     */
    sealed interface CalleeBinding extends Binding permits FunctionBinding, OverloadedFunctionBinding {
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
    record FunctionBinding(@Nonnull IFunctionFragment function) implements CalleeBinding {
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

    /**
     * An overloaded function, with the variants that its scope declares after it. The variants are added as the
     * statements of the scope are inferred, so a binding is a mutable part of a scope, as the names are. A call sees
     * only the variants declared before it, so it takes a copy of them, see {@link #variants()}.
     */
    static final class OverloadedFunctionBinding implements CalleeBinding {
        @Nonnull
        private final IOverloadedFunctionFragment function;
        @Nonnull
        private final List<IFunctionFragment> variants = new ArrayList<>();

        OverloadedFunctionBinding(@Nonnull final IOverloadedFunctionFragment function) {
            this.function = function;
        }

        /**
         * Returns the overloaded function.
         *
         * @return the declaration of the overloaded function
         */
        @Nonnull
        IOverloadedFunctionFragment function() {
            return function;
        }

        /**
         * Returns the variants declared so far.
         *
         * @return the variants in the order of their declaration
         */
        @Nonnull
        ImmutableList<IFunctionFragment> variants() {
            return ImmutableList.copyOf(variants);
        }

        /**
         * Adds a variant. No two variants of a function have the same key, so a call with type arguments selects one
         * variant at most.
         */
        private void addVariant(@Nonnull final IFunctionFragment variant) {
            final List<IType> key = typeArguments(variant);
            Preconditions.checkArgument(variants.stream().noneMatch(other -> typeArguments(other).equals(key)),
                    "two variants of an overloaded function cannot have the same type arguments");
            variants.add(variant);
        }

        @Nonnull
        private static List<IType> typeArguments(@Nonnull final IFunctionFragment variant) {
            // only variants are added
            return variant.variant().orElseThrow().typeArguments();
        }
    }
}
