/*
 * TypeInference.java
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

import com.apple.foundationdb.record.ir.fragments.IArrayType;
import com.apple.foundationdb.record.ir.fragments.IFunctionFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.apple.foundationdb.record.ir.fragments.IMetaType;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IRecordType;
import com.apple.foundationdb.record.ir.fragments.IStreamType;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Infers the types of one statement, together with the closures and stream blocks in it. The visitor makes one
 * inference for each statement that it types on its own, see {@code FrlIrFragmentVisitor}. An inference finds the
 * types that a program leaves out:
 * <ul>
 *     <li>the type of each literal and of each record constructor;</li>
 *     <li>the type arguments of each reference to a generic function;</li>
 *     <li>the parameter types and the result type of each closure that does not state them;</li>
 *     <li>the variant of each call or reference of an overloaded function, also of each operator.</li>
 * </ul>
 *
 * <p>A type that the visitor does not know yet is an inference variable, a {@link Variable}. A {@link Term} is a type
 * that can contain inference variables. FRL-IR has no subtypes and no type promotion. So each requirement on types is
 * an equation: two terms must stand for the same type. {@link #unify} solves each equation immediately. It binds open
 * variables, or it fails if the two terms cannot stand for the same type. The order of the equations does not change
 * the result.</p>
 *
 * <p>An inference has three phases:</p>
 * <ol>
 *     <li>The visitor makes terms, and it states equations between them with {@link #unify}.</li>
 *     <li>The visitor calls {@link #solve()}, which chooses the open variants of overloaded functions, and which gives
 *     a default type to each open variable with a literal constraint.</li>
 *     <li>The builders of the fragments call {@link #resolve}, which converts each term to an {@link IType}.</li>
 * </ol>
 *
 * <p>Example: the third line of this program states no type.</p>
 * <pre>{@code
 * extern func apply<T, U>(f: (T) -> U, x: T) -> U
 * extern func h(y: bigint) -> string
 * let s = apply({ x in h(x) }, 42)
 * }</pre>
 * In the steps below, {@code vT}, {@code vU}, and the other names that start with {@code v} are inference variables.
 * {@code A := B} means that {@link #unify} binds the variable {@code A} to the term {@code B}.
 * <ol>
 *     <li>The reference to {@code apply} gets the term {@code ((vT) -> vU, vT) -> vU}, see {@link #instantiate}.</li>
 *     <li>The call unifies this term with {@code (vP1, vP2) -> vR}, which gives {@code vP1 := (vT) -> vU},
 *     {@code vT := vP2}, and {@code vU := vR}. The visitor does this before it infers the arguments. So each argument
 *     has an expected type.</li>
 *     <li>The closure gets the term {@code (vX) -> vC}. The visitor unifies it with the expected type {@code vP1},
 *     which gives {@code vX := vP2} and {@code vC := vR}.</li>
 *     <li>The body {@code h(x)} gives a string, and this value is the result of the closure: {@code vR := string}. The
 *     argument {@code x} must be a bigint: {@code vP2 := bigint}.</li>
 *     <li>The literal {@code 42} gets {@code vL}, whose constraint permits only numeric types. Its expected type is
 *     {@code vP2}, which is a bigint now: {@code vL := bigint}.</li>
 *     <li>{@link #solve()} changes nothing, because no variable is open.</li>
 *     <li>{@link #resolve} gives the types: {@code x} is a bigint, the closure is {@code (bigint) -> string},
 *     {@code 42} is a bigint, the type arguments of {@code apply} are {@code bigint} and {@code string}, and {@code s}
 *     is a string.</li>
 * </ol>
 *
 * <p>The unit of inference is a statement outside closures and stream blocks, together with each closure and each
 * stream block in it. Their statements use the inference of the statement that they are in, because the context of a
 * closure can come from any part of that statement, as in {@code apply({ x in x }, 1)}. Each other statement has its
 * own inference, also each statement of a function body. So {@code let a = 42} in a function body makes {@code a} an
 * int. In a closure, a later {@code let b: bigint = a} can still make {@code a} a bigint.</p>
 *
 * <p>Nullability is a property of a type, not a wrapper around a type. So a type parameter {@code T} that occurs as
 * {@code T?} stands for its type, made nullable. The equation {@code T? = int?} makes {@code T} an {@code int}, unless
 * {@code T} must be nullable itself.</p>
 *
 * <p>A literal and a record constructor are nullable only if an equation requires it. So {@code let x = 42} makes
 * {@code 42} an {@code int}, and {@code let x: int? = 42} makes it an {@code int?}. The variable of a record
 * constructor has the record of its fields as its structure, see {@link #recordConstructor}: {@code (1, 2)} is a
 * {@code record<int, int>}, and in {@code let r: record<int, int>? = (1, 2)} it is a {@code record<int, int>?}.</p>
 *
 * <p>A call or a reference of an overloaded function needs one more step: the choice of its variant, see
 * {@link #choose}. The reference gets the types of the overloaded function, with a new variable for each type
 * parameter, as for a generic function, and a new variable for the result type, which each variant states. The
 * choice waits until exactly one variant fits these variables, that is, until the key of the variant and its result
 * type can be the types of the variables. A trial unification that is undone afterwards tests each variant. The choice
 * then binds the variables to the types of the variant. There is no ranking, and the default type of a literal does
 * not count, so {@link #solve()} fails if a choice is still open.</p>
 *
 * <p>Example: after {@code overloaded func add<A, B>(a: A, b: B)} with the variants {@code add<int, int> -> int} and
 * {@code add<bigint, bigint> -> bigint}, the call {@code x + 1} with {@code x: int} gives the term
 * {@code (vA, vB) -> vR}. The argument {@code x} gives {@code vA := int}, so only the first variant fits, and the
 * choice gives {@code vB := int} and {@code vR := int}. In {@code 1 + 2}, both variants fit, and the inference
 * fails.</p>
 */
final class TypeInference {

    /**
     * The order in which an open variable picks its default type in {@link #solve()}: the first kind in this list
     * that its constraint permits. So an integer literal gets {@code int}, and a floating-point literal gets
     * {@code double}.
     */
    @Nonnull
    private static final ImmutableList<IPrimitiveType.Kind> DEFAULT_KINDS = ImmutableList.of(IPrimitiveType.Kind.INT,
            IPrimitiveType.Kind.DOUBLE, IPrimitiveType.Kind.STRING, IPrimitiveType.Kind.BOOLEAN);

    /**
     * The message of the exception if a field access names no field of its record.
     */
    @Nonnull
    static final String NO_SUCH_FIELD = "a field access needs the name or the ordinal of a field of its record";

    /**
     * The message of the exception if no variant of an overloaded function fits a call or a reference of it.
     */
    @Nonnull
    private static final String NO_FITTING_VARIANT =
            "no variant of the overloaded function fits the types of the call or the reference";

    /**
     * The message of the exception if more than one variant of an overloaded function fits a call or a reference of it.
     */
    @Nonnull
    private static final String AMBIGUOUS_VARIANT = "more than one variant of the overloaded function fits the types "
            + "of the call or the reference; type arguments select one";

    /**
     * The inference variables that this inference made, in the order of their creation. {@link #solve()} examines
     * them.
     */
    @Nonnull
    private final List<Variable> variables = new ArrayList<>();

    /**
     * The choices of variants of overloaded functions that this inference makes, in the order of their creation, see
     * {@link #choose}.
     */
    @Nonnull
    private final List<Choice> choices = new ArrayList<>();

    /**
     * The earlier states of the variables that the current trial unification changed, the latest last, or
     * {@code null} if no trial unification is running, see {@link #wouldUnify}.
     */
    @Nullable
    private List<VariableState> trail;

    /**
     * Returns a term for a known type. Such a term contains no variables, so each inference can use it, and no
     * inference can change it. The visitor uses it, for example, for a stated type, as in {@code let x: bigint = 42},
     * and for the type of a function parameter.
     *
     * @param type the type
     * @return a term without inference variables
     */
    @Nonnull
    static Term known(@Nonnull final IType type) {
        return new Known(type);
    }

    /**
     * Returns whether a term contains no open variable, so that {@link #resolve} cannot fail for it. The visitor uses
     * this method before it copies the type of a variable of another inference.
     *
     * @param term the term
     * @return {@code true} if the term can be resolved without solving anything
     */
    static boolean isResolved(@Nonnull final Term term) {
        final Term pruned = prune(term);
        if (pruned instanceof Known) {
            return true;
        }
        if (pruned instanceof ArrayTerm arrayTerm) {
            return isResolved(arrayTerm.elementType());
        }
        if (pruned instanceof RecordTerm recordTerm) {
            return recordTerm.fields().stream().allMatch(field -> isResolved(field.type()));
        }
        if (pruned instanceof FunctionTerm functionTerm) {
            return functionTerm.parameterTypes().stream().allMatch(TypeInference::isResolved)
                    && isResolved(functionTerm.resultType());
        }
        if (pruned instanceof StreamTerm streamTerm) {
            return isResolved(streamTerm.elementType());
        }
        if (pruned instanceof MetaTerm metaTerm) {
            return isResolved(metaTerm.type());
        }
        // an open variable
        return false;
    }

    /**
     * Returns a term for the type of a literal: a new variable with the constraint of the literal kind. The constraint
     * permits only the types that the literal can have. Example: {@code 42} gets a variable that can become a numeric
     * type, and {@code nil} gets a variable that can become only a nullable type.
     *
     * @param kind the kind of the literal
     * @return a term for the type of the literal
     */
    @Nonnull
    Term literal(@Nonnull final LiteralKind kind) {
        return new VariableTerm(newVariable(kind.constraint), false);
    }

    /**
     * Returns a term for the type of a record constructor: a new variable whose constraint has the record of the
     * fields as its structure. The variable can become that record, or that record made nullable. So an equation can
     * make the record nullable, as it can make a literal nullable, and without one, {@link #solve()} makes it the
     * record that is not nullable. A field access needs a record that is not nullable, so it makes the record not
     * nullable, see {@link #fieldType}. Example: {@code (1, nil)} gets a variable that can become
     * {@code record<vA, vB>} or {@code record<vA, vB>?}, where {@code vA} and {@code vB} are the variables of the two
     * literals.
     *
     * @param fieldTypes the terms of the types of the fields, in order
     * @param names the names of the fields, in order, and an empty name for a field without one
     * @return a term for the type of the record constructor
     */
    @Nonnull
    Term recordConstructor(@Nonnull final List<Term> fieldTypes, @Nonnull final List<Optional<String>> names) {
        final ImmutableList.Builder<FieldTerm> fields = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.size(); i++) {
            fields.add(new FieldTerm(fieldTypes.get(i), names.get(i), Optional.empty()));
        }
        return new VariableTerm(newVariable(new Constraint(null, new RecordTerm(false, fields.build()), false)),
                false);
    }

    /**
     * Returns a term for the type of a record constructor whose type arguments state its fields, as in
     * {@code record.of<id(1): bigint>(id: 5)}: a new variable whose constraint has the stated record type as its
     * structure. The type arguments state the types, the names, and the numbers of the fields, but not the
     * nullability, so the context can still make the record nullable, as for {@link #recordConstructor(List, List)}.
     *
     * @param type the record type that the type arguments state; its nullability does not count
     * @return a term for the type of the record constructor
     */
    @Nonnull
    Term recordConstructor(@Nonnull final IRecordType type) {
        // a known record type decomposes to a record term
        final RecordTerm structure = (RecordTerm)decompose(new Known(type.withNullability(false)));
        return new VariableTerm(newVariable(new Constraint(null, structure, false)), false);
    }

    /**
     * Returns a term for a type that is not known yet: a new variable without a constraint. Example: the visitor makes
     * one for the result type of a call, and one for each parameter of a closure that does not state its type.
     *
     * @return a term for an unknown type
     */
    @Nonnull
    Term unknown() {
        return new VariableTerm(newVariable(Constraint.ANY), false);
    }

    /**
     * Returns a term for a function type that is not nullable.
     *
     * @param parameterTypes the terms for the types of the parameters
     * @param resultType the term for the result type
     * @return a term for the function type
     */
    @Nonnull
    Term function(@Nonnull final List<Term> parameterTypes, @Nonnull final Term resultType) {
        return new FunctionTerm(false, ImmutableList.copyOf(parameterTypes), resultType);
    }

    /**
     * Returns a term for a stream type.
     *
     * @param elementType the term for the type of the elements
     * @return a term for the stream type
     */
    @Nonnull
    Term stream(@Nonnull final Term elementType) {
        return new StreamTerm(elementType);
    }

    /**
     * Returns a term for a meta type, the type of a type token. A token is never nullable.
     *
     * @param type the term for the type that the token stands for
     * @return a term for the meta type
     */
    @Nonnull
    Term meta(@Nonnull final Term type) {
        return new MetaTerm(type);
    }

    /**
     * Returns a term for an array type that is not nullable.
     *
     * @param elementType the term for the type of the elements
     * @return a term for the array type
     */
    @Nonnull
    Term array(@Nonnull final Term elementType) {
        return new ArrayTerm(false, elementType);
    }

    /**
     * Returns the type of a reference to a generic function. The method makes a new variable for each type parameter,
     * and it puts this variable in each place of the type parameter. These variables are the type arguments of the
     * reference. Each reference makes new variables, so each call can have different type arguments.
     *
     * <p>Example: for {@code apply<T, U>(f: (T) -> U, x: T) -> U}, the method makes {@code vT} and {@code vU}. It
     * returns the term {@code ((vT) -> vU, vT) -> vU} and the type arguments {@code vT} and {@code vU}. The two
     * places of {@code T} hold two different {@code VariableTerm} records, but both refer to {@code vT}. So one
     * binding of {@code vT} changes both places.</p>
     *
     * @param typeParameters the type parameters of the function
     * @param type the type of the function, in terms of its type parameters
     * @return the type of the reference and the type arguments
     */
    @Nonnull
    Instantiation instantiate(@Nonnull final List<IGenericType> typeParameters, @Nonnull final IFunctionType type) {
        final Map<String, Variable> substitution = substitution(typeParameters);
        return new Instantiation(toTerm(type, substitution), typeArguments(typeParameters, substitution));
    }

    /**
     * Returns the type of a reference to an overloaded function, whose variant is not chosen yet. The method makes a
     * new variable for each type parameter, as for a generic function. These variables are the type arguments of the
     * reference, which {@link #choose} relates to the keys of the variants. The result type is the given term, since
     * each variant states its own result type.
     *
     * <p>Example: for {@code overloaded func add<A, B>(a: A, b: B)} and the result type {@code vR}, the method makes
     * {@code vA} and {@code vB}, and it returns the term {@code (vA, vB) -> vR}.</p>
     *
     * @param typeParameters the type parameters of the overloaded function
     * @param parameterTypes the types of the parameters, in terms of the type parameters
     * @param resultType the term of the result type of the reference
     * @return the type of the reference and the type arguments
     */
    @Nonnull
    Instantiation instantiate(@Nonnull final List<IGenericType> typeParameters,
                              @Nonnull final List<IType> parameterTypes, @Nonnull final Term resultType) {
        final Map<String, Variable> substitution = substitution(typeParameters);
        return new Instantiation(new FunctionTerm(false, parameterTypes.stream()
                .map(parameterType -> toTerm(parameterType, substitution))
                .collect(ImmutableList.toImmutableList()), resultType), typeArguments(typeParameters, substitution));
    }

    /**
     * Returns a new variable for each type parameter, by the name of the type parameter.
     */
    @Nonnull
    private Map<String, Variable> substitution(@Nonnull final List<IGenericType> typeParameters) {
        final ImmutableMap.Builder<String, Variable> substitution = ImmutableMap.builder();
        for (final IGenericType typeParameter : typeParameters) {
            substitution.put(typeParameter.name(), newVariable(Constraint.ANY));
        }
        return substitution.build();
    }

    /**
     * Returns the terms of the variables of the type parameters, in the order of the type parameters.
     */
    @Nonnull
    private static ImmutableList<Term> typeArguments(@Nonnull final List<IGenericType> typeParameters,
                                                     @Nonnull final Map<String, Variable> substitution) {
        return typeParameters.stream()
                .<Term>map(typeParameter -> new VariableTerm(substitution.get(typeParameter.name()), false))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Starts the choice of the variant of an overloaded function for a call or a reference of it. The choice is made
     * as soon as exactly one variant fits, see {@link #propagate()}, and at the latest in {@link #solve()}, which fails
     * if none or more than one variant fits. A variant fits if its key can be the type arguments of the reference and
     * its result type can be the result type of the reference.
     *
     * <p>Example: in {@code x + 1} with {@code x: int} and the variants {@code add<int, int> -> int} and
     * {@code add<bigint, bigint> -> bigint}, only the first variant fits once the argument {@code x} gives
     * {@code vA := int}. Explicit type arguments, as in {@code x +<int, int> 1}, bind the variables before the choice,
     * so at most one variant fits, because no two variants have the same key.</p>
     *
     * @param typeArguments the terms of the type arguments of the reference, see {@link #instantiate}
     * @param resultType the term of the result type of the reference
     * @param variants the variants that the reference can call: those declared before the reference
     * @return the choice, whose variant is known after {@link #solve()}
     */
    @Nonnull
    Choice choose(@Nonnull final List<Term> typeArguments, @Nonnull final Term resultType,
                  @Nonnull final List<IFunctionFragment> variants) {
        final Choice choice =
                new Choice(ImmutableList.copyOf(typeArguments), resultType, ImmutableList.copyOf(variants));
        choices.add(choice);
        return choice;
    }

    /**
     * Makes each open choice of a variant that exactly one variant fits, see {@link #choose}. A choice binds variables,
     * so it can decide other choices; the method repeats until no choice changes. The visitor calls it after each call,
     * so that the type of a call of an overloaded function is known as early as possible, for example for a field
     * access on its result.
     *
     * <p>More equations can only remove variants from those that fit, so a choice that one variant fits now stays with
     * that variant, and a choice that no variant fits fails at once.</p>
     *
     * @throws IllegalArgumentException if no variant fits an open choice
     */
    void propagate() {
        boolean changed = true;
        while (changed) {
            changed = false;
            for (final Choice choice : choices) {
                if (choice.chosen == null) {
                    final List<IFunctionFragment> fitting = choice.variants.stream()
                            .filter(variant -> wouldUnify(choice.terms(), variantTerms(variant)))
                            .collect(ImmutableList.toImmutableList());
                    if (fitting.isEmpty()) {
                        throw new IllegalArgumentException(NO_FITTING_VARIANT);
                    }
                    if (fitting.size() == 1) {
                        final IFunctionFragment variant = fitting.get(0);
                        final List<Term> terms = choice.terms();
                        final List<Term> types = variantTerms(variant);
                        for (int i = 0; i < terms.size(); i++) {
                            // the trial unification of the same terms succeeded
                            unify(terms.get(i), types.get(i), NO_FITTING_VARIANT);
                        }
                        choice.chosen = variant;
                        changed = true;
                    }
                }
            }
        }
    }

    /**
     * Returns the terms of the key and of the result type of a variant, in the order of {@link Choice#terms()}.
     */
    @Nonnull
    private static List<Term> variantTerms(@Nonnull final IFunctionFragment variant) {
        // only variants are candidates of a choice
        final ImmutableList.Builder<Term> terms = ImmutableList.builder();
        variant.variant().orElseThrow().typeArguments().forEach(typeArgument -> terms.add(new Known(typeArgument)));
        return terms.add(new Known(variant.resultType())).build();
    }

    /**
     * Returns whether each pair of terms can stand for the same type, all pairs at once. The method unifies the pairs
     * on trial, and it then undoes every change, so it binds no variable, also if the unification fails.
     */
    private boolean wouldUnify(@Nonnull final List<Term> firsts, @Nonnull final List<Term> seconds) {
        Preconditions.checkState(trail == null, "a trial unification cannot start within another one");
        final List<VariableState> changes = new ArrayList<>();
        trail = changes;
        try {
            for (int i = 0; i < firsts.size(); i++) {
                if (!tryUnify(firsts.get(i), seconds.get(i))) {
                    return false;
                }
            }
            return true;
        } finally {
            trail = null;
            for (int i = changes.size() - 1; i >= 0; i--) {
                final VariableState state = changes.get(i);
                state.variable().binding = state.binding();
                state.variable().constraint = state.constraint();
            }
        }
    }

    /**
     * Requires two terms to stand for the same type. The method binds open variables of the two terms where
     * necessary. It changes only variables, and it binds each variable one time at most. One call can bind several
     * variables. Example: the equation {@code (vA, vB) -> vC = (int, vD) -> [string]} gives {@code vA := int},
     * {@code vB := vD}, and {@code vC := [string]}.
     *
     * <p>If the equation fails, the method does not undo the bindings that it made before the failure. The exception
     * then stops the parse, so no code uses that state.</p>
     *
     * <p>The method can bind each variable in the two terms, also a variable of another inference. So the visitor
     * gives it only terms of the current inference and copies of resolved types.</p>
     *
     * @param first a term
     * @param second another term
     * @param message the message of the exception if the two cannot be the same type
     * @throws IllegalArgumentException if the two terms cannot stand for the same type
     */
    void unify(@Nonnull final Term first, @Nonnull final Term second, @Nonnull final String message) {
        if (!tryUnify(first, second)) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Makes the open choices of variants, and then gives a default type to each open variable whose constraint has a
     * default type, see {@link #DEFAULT_KINDS}. Only the constraints of number, string, and boolean literals, and of
     * record constructors, have one: the default type of a record constructor is its record, not nullable unless the
     * constraint requires that. The visitor calls this method after the statement stated all of its equations, and
     * before the builders resolve the terms. The method can run more than one time, and each call changes only the
     * variables that are open at that time. A for loop with its own inference uses this, because it must know the
     * type of its variable before it infers its body.
     *
     * <p>The choices come first, and each must have exactly one variant that fits, see {@link #propagate()}. So the
     * default type of a literal never selects a variant: in {@code 1 + 2}, the literals can have the types of more than
     * one variant, and the method fails.</p>
     *
     * <p>Example: in {@code let x = 42}, no equation fixes the variable of {@code 42}, so this method binds it to
     * {@code int}. In {@code let x = nil}, the variable of {@code nil} has no default type. It stays open, and
     * {@link #resolve} fails.</p>
     *
     * @throws IllegalArgumentException if no variant or more than one variant fits a choice
     */
    void solve() {
        propagate();
        Preconditions.checkArgument(choices.stream().allMatch(choice -> choice.chosen != null), AMBIGUOUS_VARIANT);
        for (final Variable variable : variables) {
            final Optional<Term> defaultType = variable.constraint.defaultType();
            if (variable.binding == null && defaultType.isPresent()) {
                variable.binding = defaultType.get();
            }
        }
    }

    /**
     * Returns the type that a term stands for. The method follows the bindings of the variables in the term, and it
     * converts the result to an {@link IType}. Call it only after {@link #solve()} ran for the inference of these
     * variables. Example: if {@code vX := vP2} and {@code vP2 := bigint}, the term {@code (vX) -> vX} resolves to
     * {@code (bigint) -> bigint}.
     *
     * @param term the term
     * @return the type of the term
     * @throws IllegalArgumentException if a variable in the term is still open, as for {@code let x = nil}
     */
    @Nonnull
    static IType resolve(@Nonnull final Term term) {
        final Term pruned = prune(term);
        if (pruned instanceof Known known) {
            return known.type();
        }
        if (pruned instanceof ArrayTerm arrayTerm) {
            return new IArrayType(arrayTerm.isNullable(), resolve(arrayTerm.elementType()));
        }
        if (pruned instanceof RecordTerm recordTerm) {
            return new IRecordType(recordTerm.isNullable(), recordTerm.fields()
                    .stream()
                    .map(field -> new IRecordType.Field(resolve(field.type()), field.name(), field.number()))
                    .collect(ImmutableList.toImmutableList()));
        }
        if (pruned instanceof FunctionTerm functionTerm) {
            return new IFunctionType(functionTerm.isNullable(), functionTerm.parameterTypes()
                    .stream()
                    .map(TypeInference::resolve)
                    .collect(ImmutableList.toImmutableList()), resolve(functionTerm.resultType()));
        }
        if (pruned instanceof StreamTerm streamTerm) {
            return new IStreamType(resolve(streamTerm.elementType()));
        }
        if (pruned instanceof MetaTerm metaTerm) {
            return new IMetaType(resolve(metaTerm.type()));
        }
        // an open variable: no equation of the statement determines it, and it has no default
        throw new IllegalArgumentException("the type cannot be inferred");
    }

    /**
     * Returns the ordinal of the field with the given name in the record that a term stands for. Example: in
     * {@code record<id: bigint, name: string?>}, the field {@code name} has the ordinal 1.
     *
     * @param record the term of the record, which must stand for a record structure, see {@link #recordFields}
     * @param name the name of the field
     * @return the ordinal of the field, its position in the record type
     * @throws IllegalArgumentException if the term does not stand for a record that is not nullable, or if the record
     *         has no field of this name
     */
    int fieldOrdinal(@Nonnull final Term record, @Nonnull final String name) {
        final List<FieldTerm> fields = recordFields(record);
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(Optional.of(name))) {
                return i;
            }
        }
        throw new IllegalArgumentException(NO_SUCH_FIELD);
    }

    /**
     * Returns the term of the type of the field at an ordinal of the record that a term stands for. The term of the
     * field can still contain variables. Example: if the record has the term {@code record<a: vT, b: int>}, the field
     * at ordinal 0 has the term {@code vT}, and a later equation of the statement can bind {@code vT}.
     *
     * @param record the term of the record, which must stand for a record structure, see {@link #recordFields}
     * @param ordinal the position of the field in the record type, starting at 0
     * @return the term of the type of the field
     * @throws IllegalArgumentException if the term does not stand for a record that is not nullable, or if the record
     *         has no field at this ordinal
     */
    @Nonnull
    Term fieldType(@Nonnull final Term record, final int ordinal) {
        final List<FieldTerm> fields = recordFields(record);
        Preconditions.checkArgument(ordinal >= 0 && ordinal < fields.size(), NO_SUCH_FIELD);
        return fields.get(ordinal).type();
    }

    /**
     * Returns the fields of the record that a term stands for. The term must stand for a record structure: a known
     * record type, a record term whose field types can still contain variables, or the type of a record constructor,
     * which the access makes not nullable. An open variable is not enough otherwise, because many record types can
     * have a field with a given name. So the visitor requires the record structure to be known where a program
     * accesses a field, as Swift does.
     */
    @Nonnull
    private List<FieldTerm> recordFields(@Nonnull final Term record) {
        final Term decomposed = decompose(prune(record));
        if (decomposed instanceof VariableTerm variableTerm && canBeRecord(variableTerm)) {
            final RecordTerm structure = variableTerm.variable().constraint.structure();
            if (structure == null) {
                throw new IllegalArgumentException("the type of a record must be known where a field of it is "
                        + "accessed");
            }
            // the variable of a record constructor, whose context left its nullability open: a field access needs a
            // record that is not nullable, which the structure is
            setBinding(variableTerm.variable(), structure);
            return structure.fields();
        }
        if (!(decomposed instanceof RecordTerm recordTerm) || recordTerm.isNullable()) {
            throw new IllegalArgumentException("a field access needs a record that is not nullable");
        }
        return recordTerm.fields();
    }

    /**
     * Returns whether an open variable can still get a record type that is not nullable. It cannot if it occurs as
     * {@code T?}, or if its constraint permits only primitive types or only nullable types. Examples: the literal
     * {@code 1} gets a variable whose constraint permits only numeric types, and {@code nil} gets a variable whose
     * constraint permits only nullable types. So {@code 1.f} and {@code nil.f} fail because the value is not a record
     * that is not nullable, not because its type is not known.
     */
    private static boolean canBeRecord(@Nonnull final VariableTerm variableTerm) {
        final Constraint constraint = variableTerm.variable().constraint;
        return !variableTerm.isNullable() && constraint.kinds() == null && !constraint.isNullable();
    }

    @Nonnull
    private Variable newVariable(@Nonnull final Constraint constraint) {
        final Variable variable = new Variable(constraint);
        variables.add(variable);
        return variable;
    }

    private boolean tryUnify(@Nonnull final Term first, @Nonnull final Term second) {
        final Term a = prune(first);
        final Term b = prune(second);
        if (a instanceof VariableTerm variableTerm) {
            return bind(variableTerm, b);
        }
        if (b instanceof VariableTerm variableTerm) {
            return bind(variableTerm, a);
        }
        if (a instanceof Known knownA && b instanceof Known knownB) {
            return knownA.type().equals(knownB.type());
        }
        return unifyParts(decompose(a), decompose(b));
    }

    /**
     * Unifies two terms of the same structure part by part.
     */
    private boolean unifyParts(@Nonnull final Term a, @Nonnull final Term b) {
        if (a instanceof ArrayTerm arrayA && b instanceof ArrayTerm arrayB) {
            return arrayA.isNullable() == arrayB.isNullable() && tryUnify(arrayA.elementType(), arrayB.elementType());
        }
        if (a instanceof RecordTerm recordA && b instanceof RecordTerm recordB) {
            if (recordA.isNullable() != recordB.isNullable() || recordA.fields().size() != recordB.fields().size()) {
                return false;
            }
            for (int i = 0; i < recordA.fields().size(); i++) {
                final FieldTerm fieldA = recordA.fields().get(i);
                final FieldTerm fieldB = recordB.fields().get(i);
                if (!fieldA.name().equals(fieldB.name()) || !fieldA.number().equals(fieldB.number())
                        || !tryUnify(fieldA.type(), fieldB.type())) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof FunctionTerm functionA && b instanceof FunctionTerm functionB) {
            if (functionA.isNullable() != functionB.isNullable()
                    || functionA.parameterTypes().size() != functionB.parameterTypes().size()) {
                return false;
            }
            for (int i = 0; i < functionA.parameterTypes().size(); i++) {
                if (!tryUnify(functionA.parameterTypes().get(i), functionB.parameterTypes().get(i))) {
                    return false;
                }
            }
            return tryUnify(functionA.resultType(), functionB.resultType());
        }
        if (a instanceof StreamTerm streamA && b instanceof StreamTerm streamB) {
            return tryUnify(streamA.elementType(), streamB.elementType());
        }
        if (a instanceof MetaTerm metaA && b instanceof MetaTerm metaB) {
            return tryUnify(metaA.type(), metaB.type());
        }
        return false;
    }

    /**
     * Binds the variable of a variable term so that the term stands for the target, if the constraint of the variable
     * and the absence of promotions allow that.
     */
    private boolean bind(@Nonnull final VariableTerm variableTerm, @Nonnull final Term target) {
        if (target instanceof VariableTerm targetVariableTerm) {
            return link(variableTerm, targetVariableTerm);
        }
        final Variable variable = variableTerm.variable();
        if (occurs(variable, target)) {
            return false;
        }
        final Term value;
        if (variableTerm.isNullable()) {
            // T? stands for the target, which must be nullable then; T is the target itself only if T must be
            // nullable, and the target without its nullability otherwise
            final Term nonNullableTarget = withoutNullability(target);
            if (nonNullableTarget == null) {
                return false;
            }
            value = variable.constraint.isNullable() ? target : nonNullableTarget;
        } else {
            value = target;
        }
        if (!allows(variable.constraint, value)) {
            return false;
        }
        // the variable of a record constructor stands for its structure, made nullable or not
        final RecordTerm structure = variable.constraint.structure();
        final Term nonNullableValue = withoutNullability(value);
        if (structure != null && !tryUnify(structure, nonNullableValue == null ? value : nonNullableValue)) {
            return false;
        }
        setBinding(variable, value);
        return true;
    }

    /**
     * Unifies two variable terms: one variable becomes a term over the other one.
     */
    private boolean link(@Nonnull final VariableTerm first, @Nonnull final VariableTerm second) {
        if (first.variable() == second.variable()) {
            // T and T? are the same type only if T is nullable itself
            return first.isNullable() == second.isNullable() || constrain(first.variable(), Constraint.NULLABLE);
        }
        if (first.isNullable() && !second.isNullable()) {
            // T? = U: U is T made nullable
            return bindToVariable(second.variable(), new VariableTerm(first.variable(), true));
        }
        // T = U and T = U?, and T? = U?, which T = U solves
        return bindToVariable(first.variable(),
                new VariableTerm(second.variable(), second.isNullable() && !first.isNullable()));
    }

    /**
     * Binds a variable to a term over another variable. The constraint of the variable moves to the other variable,
     * without the requirement to be nullable if the term is the nullable version of the other variable, which meets it.
     * The binding fails if the variable occurs in the structure of the other variable, as in {@code T = (a: T)}.
     */
    private boolean bindToVariable(@Nonnull final Variable variable, @Nonnull final VariableTerm target) {
        if (occurs(variable, target)) {
            return false;
        }
        final Constraint moved = target.isNullable() ? variable.constraint.withoutNullability() : variable.constraint;
        if (!constrain(target.variable(), moved)) {
            return false;
        }
        setBinding(variable, target);
        return true;
    }

    /**
     * Adds a constraint to a variable, if a type can meet both the new and the existing one. If both constraints have
     * a structure, the two structures must stand for the same record. The variable must not occur in its structure,
     * which would be an infinite type.
     */
    private boolean constrain(@Nonnull final Variable variable, @Nonnull final Constraint constraint) {
        final Optional<Constraint> combined = variable.constraint.and(constraint);
        if (combined.isEmpty()) {
            return false;
        }
        final RecordTerm structure = variable.constraint.structure();
        final RecordTerm otherStructure = constraint.structure();
        if (structure != null && otherStructure != null && !tryUnify(structure, otherStructure)) {
            return false;
        }
        final RecordTerm combinedStructure = combined.get().structure();
        if (combinedStructure != null && occurs(variable, combinedStructure)) {
            return false;
        }
        remember(variable);
        variable.constraint = combined.get();
        return true;
    }

    /**
     * Binds a variable, which a running trial unification undoes later.
     */
    private void setBinding(@Nonnull final Variable variable, @Nonnull final Term binding) {
        remember(variable);
        variable.binding = binding;
    }

    /**
     * Records the state of a variable before a change, if a trial unification is running, see {@link #wouldUnify}.
     */
    private void remember(@Nonnull final Variable variable) {
        if (trail != null) {
            trail.add(new VariableState(variable, variable.binding, variable.constraint));
        }
    }

    /**
     * Returns whether a constraint allows a term that is not a variable term, as far as its kinds and its nullability
     * go. {@link #bind} checks the structure of a constraint.
     */
    private static boolean allows(@Nonnull final Constraint constraint, @Nonnull final Term term) {
        if (term instanceof Known known) {
            return constraint.allows(known.type());
        }
        // an array, record, function, or stream type: only a constraint without kinds allows one, such as the one of
        // nil or of a record constructor
        return constraint.kinds() == null && (!constraint.isNullable() || isNullable(term));
    }

    private static boolean isNullable(@Nonnull final Term term) {
        if (term instanceof ArrayTerm arrayTerm) {
            return arrayTerm.isNullable();
        }
        if (term instanceof RecordTerm recordTerm) {
            return recordTerm.isNullable();
        }
        if (term instanceof FunctionTerm functionTerm) {
            return functionTerm.isNullable();
        }
        // a stream type or a meta type, which is never nullable
        return false;
    }

    /**
     * Returns a term that is not a variable term without its nullability, or {@code null} if it is not nullable.
     */
    @Nullable
    private static Term withoutNullability(@Nonnull final Term term) {
        if (term instanceof Known known) {
            return known.type().isNullable() ? new Known(known.type().withNullability(false)) : null;
        }
        if (term instanceof ArrayTerm arrayTerm) {
            return arrayTerm.isNullable() ? new ArrayTerm(false, arrayTerm.elementType()) : null;
        }
        if (term instanceof RecordTerm recordTerm) {
            return recordTerm.isNullable() ? new RecordTerm(false, recordTerm.fields()) : null;
        }
        if (term instanceof FunctionTerm functionTerm) {
            return functionTerm.isNullable()
                    ? new FunctionTerm(false, functionTerm.parameterTypes(), functionTerm.resultType())
                    : null;
        }
        // a stream type or a meta type, which is never nullable
        return null;
    }

    /**
     * Returns whether a variable occurs in a term. A variable must not be bound to a term it occurs in, as in
     * {@code T = [T]}: that would be an infinite type. The structure of an open variable is a part of its type, so the
     * variable also occurs in a term if it occurs in the structure of an open variable of the term.
     */
    private static boolean occurs(@Nonnull final Variable variable, @Nonnull final Term term) {
        final Term pruned = prune(term);
        if (pruned instanceof VariableTerm variableTerm) {
            final RecordTerm structure = variableTerm.variable().constraint.structure();
            return variableTerm.variable() == variable || (structure != null && occurs(variable, structure));
        }
        if (pruned instanceof ArrayTerm arrayTerm) {
            return occurs(variable, arrayTerm.elementType());
        }
        if (pruned instanceof RecordTerm recordTerm) {
            return recordTerm.fields().stream().anyMatch(field -> occurs(variable, field.type()));
        }
        if (pruned instanceof FunctionTerm functionTerm) {
            return functionTerm.parameterTypes().stream().anyMatch(parameterType -> occurs(variable, parameterType))
                    || occurs(variable, functionTerm.resultType());
        }
        if (pruned instanceof StreamTerm streamTerm) {
            return occurs(variable, streamTerm.elementType());
        }
        if (pruned instanceof MetaTerm metaTerm) {
            return occurs(variable, metaTerm.type());
        }
        return false;
    }

    /**
     * Returns the term that a term stands for at its top level. The method follows the bindings of variables until it
     * gets a term that is not a bound variable. It does not go into the parts of a compound term, and it changes
     * nothing. For a variable that occurs as {@code T?}, it makes the result nullable. Example: if
     * {@code vX := vP2} and {@code vP2 := [vT]}, the term {@code vX} prunes to {@code [vT]}, also if {@code vT} is
     * open.
     */
    @Nonnull
    private static Term prune(@Nonnull final Term term) {
        if (term instanceof VariableTerm variableTerm) {
            final Term binding = variableTerm.variable().binding;
            if (binding != null) {
                return variableTerm.isNullable() ? nullable(prune(binding)) : prune(binding);
            }
        }
        return term;
    }

    @Nonnull
    private static Term nullable(@Nonnull final Term term) {
        if (term instanceof Known known) {
            return new Known(known.type().withNullability(true));
        }
        if (term instanceof VariableTerm variableTerm) {
            return new VariableTerm(variableTerm.variable(), true);
        }
        if (term instanceof ArrayTerm arrayTerm) {
            return new ArrayTerm(true, arrayTerm.elementType());
        }
        if (term instanceof RecordTerm recordTerm) {
            return new RecordTerm(true, recordTerm.fields());
        }
        if (term instanceof FunctionTerm functionTerm) {
            return new FunctionTerm(true, functionTerm.parameterTypes(), functionTerm.resultType());
        }
        throw new IllegalArgumentException("a stream type or a meta type cannot be nullable");
    }

    /**
     * Returns a term of the structure of a known array, record, function, stream, or meta type, whose parts are terms
     * of their own, so that it can be unified part by part with a term that contains variables. Other terms are
     * returned as they are.
     */
    @Nonnull
    private static Term decompose(@Nonnull final Term term) {
        if (!(term instanceof Known known)) {
            return term;
        }
        final IType type = known.type();
        if (type instanceof IArrayType arrayType) {
            return new ArrayTerm(arrayType.isNullable(), new Known(arrayType.elementType()));
        }
        if (type instanceof IRecordType recordType) {
            return new RecordTerm(recordType.isNullable(), recordType.fields()
                    .stream()
                    .map(field -> new FieldTerm(new Known(field.type()), field.name(), field.number()))
                    .collect(ImmutableList.toImmutableList()));
        }
        if (type instanceof IFunctionType functionType) {
            return new FunctionTerm(functionType.isNullable(), functionType.parameterTypes()
                    .stream()
                    .<Term>map(Known::new)
                    .collect(ImmutableList.toImmutableList()), new Known(functionType.resultType()));
        }
        if (type instanceof IStreamType streamType) {
            return new StreamTerm(new Known(streamType.elementType()));
        }
        if (type instanceof IMetaType metaType) {
            return new MetaTerm(new Known(metaType.type()));
        }
        return term;
    }

    /**
     * Returns the term for a type in which the type parameters of the substitution are inference variables.
     */
    @Nonnull
    private static Term toTerm(@Nonnull final IType type, @Nonnull final Map<String, Variable> substitution) {
        if (substitution.isEmpty()) {
            return new Known(type);
        }
        if (type instanceof IGenericType genericType) {
            final Variable variable = substitution.get(genericType.name());
            return variable == null ? new Known(type) : new VariableTerm(variable, genericType.isNullable());
        }
        if (type instanceof IArrayType arrayType) {
            return new ArrayTerm(arrayType.isNullable(), toTerm(arrayType.elementType(), substitution));
        }
        if (type instanceof IRecordType recordType) {
            return new RecordTerm(recordType.isNullable(), recordType.fields()
                    .stream()
                    .map(field -> new FieldTerm(toTerm(field.type(), substitution), field.name(), field.number()))
                    .collect(ImmutableList.toImmutableList()));
        }
        if (type instanceof IFunctionType functionType) {
            return new FunctionTerm(functionType.isNullable(), functionType.parameterTypes()
                    .stream()
                    .map(parameterType -> toTerm(parameterType, substitution))
                    .collect(ImmutableList.toImmutableList()), toTerm(functionType.resultType(), substitution));
        }
        if (type instanceof IStreamType streamType) {
            return new StreamTerm(toTerm(streamType.elementType(), substitution));
        }
        if (type instanceof IMetaType metaType) {
            return new MetaTerm(toTerm(metaType.type(), substitution));
        }
        return new Known(type);
    }

    /**
     * The kinds of literals, with the types that each can denote, as in Swift: an integer literal can be of any numeric
     * type, and {@code nil} of any nullable type. Every literal can also be of the nullable version of such a type.
     */
    enum LiteralKind {
        INTEGER(ImmutableSet.of(IPrimitiveType.Kind.INT, IPrimitiveType.Kind.BIGINT, IPrimitiveType.Kind.HALF,
                IPrimitiveType.Kind.FLOAT, IPrimitiveType.Kind.DOUBLE), false),
        FLOATING(ImmutableSet.of(IPrimitiveType.Kind.HALF, IPrimitiveType.Kind.FLOAT, IPrimitiveType.Kind.DOUBLE), false),
        STRING(ImmutableSet.of(IPrimitiveType.Kind.STRING), false),
        BOOLEAN(ImmutableSet.of(IPrimitiveType.Kind.BOOLEAN), false),
        NIL(null, true);

        @Nonnull
        private final Constraint constraint;

        LiteralKind(@Nullable final ImmutableSet<IPrimitiveType.Kind> kinds, final boolean isNullable) {
            this.constraint = new Constraint(kinds, null, isNullable);
        }
    }

    /**
     * A type that can contain inference variables. An {@link IType} cannot contain them, so the fragments contain no
     * terms. The visitor gets terms from {@link TypeInference#known}, {@link TypeInference#unknown},
     * {@link TypeInference#literal}, {@link TypeInference#recordConstructor}, {@link TypeInference#function},
     * {@link TypeInference#array}, {@link TypeInference#stream}, {@link TypeInference#meta}, and
     * {@link TypeInference#instantiate}. It gives the terms back to {@link TypeInference}, and it does not look into
     * them. Only {@link TypeInference} does that.
     *
     * <p>A {@code Known} term holds a type without variables. A {@code VariableTerm} refers to a variable. Each type
     * that contains other types has a compound term kind, with a term for each part: {@code ArrayTerm},
     * {@code RecordTerm}, {@code FunctionTerm}, {@code StreamTerm}, and {@code MetaTerm}. A {@code VariableTerm} can be
     * in each part position. So a term can describe a type with unknown parts, for example {@code [vT]}, an array of an
     * unknown element type. The interface is sealed, and these seven records in this file are its only
     * implementations.</p>
     *
     * <p>All terms are immutable records. But a {@code VariableTerm} refers to a {@link Variable}, which is mutable. So
     * the type that a term stands for changes when {@link TypeInference#unify} binds a variable in it. Example: after
     * {@code vT := int}, the term {@code [vT]} stands for {@code [int]}, and the term itself is unchanged.</p>
     */
    sealed interface Term {
    }

    /**
     * The type of a reference to a function, with an inference variable for each type parameter of the function, see
     * {@link TypeInference#instantiate}.
     *
     * @param type the type of the reference
     * @param typeArguments the variables for the type parameters of the function, in order
     */
    record Instantiation(@Nonnull Term type, @Nonnull ImmutableList<Term> typeArguments) {
    }

    /**
     * The choice of the variant of an overloaded function for one call or reference, see {@link TypeInference#choose}.
     * The inference makes the choice, and the builder of the reference reads it once the inference is solved.
     */
    static final class Choice {
        @Nonnull
        private final ImmutableList<Term> typeArguments;
        @Nonnull
        private final Term resultType;
        @Nonnull
        private final ImmutableList<IFunctionFragment> variants;
        @Nullable
        private IFunctionFragment chosen;

        private Choice(@Nonnull final ImmutableList<Term> typeArguments, @Nonnull final Term resultType,
                       @Nonnull final ImmutableList<IFunctionFragment> variants) {
            this.typeArguments = typeArguments;
            this.resultType = resultType;
            this.variants = variants;
        }

        /**
         * Returns the chosen variant.
         *
         * @return the variant that the call or the reference calls
         * @throws IllegalStateException if the variant is not chosen yet, which is the case before
         *         {@link TypeInference#solve()}
         */
        @Nonnull
        IFunctionFragment chosen() {
            if (chosen == null) {
                throw new IllegalStateException("the variant of an overloaded function is chosen when its inference "
                        + "is solved");
            }
            return chosen;
        }

        /**
         * Returns the terms that a variant must fit: the type arguments of the reference, followed by its result type.
         */
        @Nonnull
        private List<Term> terms() {
            return ImmutableList.<Term>builder().addAll(typeArguments).add(resultType).build();
        }
    }

    /**
     * The state of a variable before a trial unification changed it, see {@link TypeInference#wouldUnify}.
     *
     * @param variable the variable
     * @param binding the binding of the variable before the change
     * @param constraint the constraint of the variable before the change
     */
    private record VariableState(@Nonnull Variable variable, @Nullable Term binding, @Nonnull Constraint constraint) {
    }

    /**
     * A type without inference variables. No inference can change it.
     */
    private record Known(@Nonnull IType type) implements Term {
    }

    /**
     * A reference to an inference variable. It stands for the type of the variable, made nullable if
     * {@code isNullable}, as {@code T?} does. Several terms can refer to one variable, so one binding changes all of
     * them.
     */
    private record VariableTerm(@Nonnull Variable variable, boolean isNullable) implements Term {
    }

    private record ArrayTerm(boolean isNullable, @Nonnull Term elementType) implements Term {
    }

    private record RecordTerm(boolean isNullable, @Nonnull ImmutableList<FieldTerm> fields) implements Term {
    }

    private record FieldTerm(@Nonnull Term type, @Nonnull Optional<String> name, @Nonnull Optional<Integer> number) {
    }

    private record FunctionTerm(boolean isNullable, @Nonnull ImmutableList<Term> parameterTypes,
                                @Nonnull Term resultType) implements Term {
    }

    private record StreamTerm(@Nonnull Term elementType) implements Term {
    }

    private record MetaTerm(@Nonnull Term type) implements Term {
    }

    /**
     * An inference variable: a type that the inference does not know yet. It is the only mutable object of an
     * inference, and only {@link TypeInference} changes it.
     * <ul>
     *     <li>{@code binding} is null while the variable is open. {@link TypeInference#unify} sets it one time, to a
     *     term of any kind: a {@code Known} term, a {@code VariableTerm} that links the variable to another open
     *     variable, or a compound term that can contain other variables. {@link TypeInference#solve()} can set it to
     *     a default type.</li>
     *     <li>{@code constraint} limits the terms that the variable can get. While the variable is open,
     *     {@link TypeInference#unify} can narrow it. When {@link TypeInference#unify} links two open variables, the
     *     constraint of the first variable moves to the second.</li>
     * </ul>
     * Example: the literal {@code 42} gets a variable whose constraint permits only numeric types. So {@code bigint} is
     * a permitted binding, and {@code string} is not.
     */
    private static final class Variable {
        @Nullable
        private Term binding;
        @Nonnull
        private Constraint constraint;

        private Variable(@Nonnull final Constraint constraint) {
            this.constraint = constraint;
        }
    }

    /**
     * What an inference variable requires of its type: a primitive type of one of the given kinds, or any type if there
     * are no kinds; if there is a structure, the record of that structure, nullable or not; and a nullable type if
     * {@code isNullable}. The variable of a record constructor has its record as its structure, so that the context
     * can make the record nullable, as it can make a literal nullable.
     *
     * @param kinds the kinds of primitive types allowed, or {@code null} for any type
     * @param structure the record, not nullable, that the type must be, made nullable or not, or {@code null} for any
     *        type; a constraint has kinds or a structure, never both
     * @param isNullable whether the type must be nullable
     */
    private record Constraint(@Nullable ImmutableSet<IPrimitiveType.Kind> kinds, @Nullable RecordTerm structure,
                              boolean isNullable) {
        @Nonnull
        private static final Constraint ANY = new Constraint(null, null, false);
        @Nonnull
        private static final Constraint NULLABLE = new Constraint(null, null, true);

        /**
         * Returns whether a type meets the kinds and the nullability of this constraint. {@link TypeInference#bind}
         * checks the structure.
         */
        private boolean allows(@Nonnull final IType type) {
            return (!isNullable || type.isNullable())
                    && (kinds == null || (type instanceof IPrimitiveType primitiveType && kinds.contains(primitiveType.kind())));
        }

        /**
         * Returns the constraint of a type that meets both this constraint and the other one, or empty if no type does.
         * If both have a structure, the result has the structure of this one, and {@link TypeInference#constrain}
         * unifies the two.
         */
        @Nonnull
        private Optional<Constraint> and(@Nonnull final Constraint other) {
            final ImmutableSet<IPrimitiveType.Kind> commonKinds;
            if (kinds == null) {
                commonKinds = other.kinds;
            } else if (other.kinds == null) {
                commonKinds = kinds;
            } else {
                commonKinds = Sets.intersection(kinds, other.kinds).immutableCopy();
            }
            if (commonKinds != null && commonKinds.isEmpty()) {
                return Optional.empty();
            }
            final RecordTerm commonStructure = structure == null ? other.structure : structure;
            if (commonKinds != null && commonStructure != null) {
                // a primitive type is not a record
                return Optional.empty();
            }
            return Optional.of(new Constraint(commonKinds, commonStructure, isNullable || other.isNullable));
        }

        @Nonnull
        private Constraint withoutNullability() {
            return new Constraint(kinds, structure, false);
        }

        /**
         * Returns the type of a literal or a record constructor that no equation determines: the first allowed kind in
         * {@link #DEFAULT_KINDS}, or the structure, nullable only if this constraint requires it.
         */
        @Nonnull
        private Optional<Term> defaultType() {
            if (structure != null) {
                return Optional.of(isNullable ? new RecordTerm(true, structure.fields()) : structure);
            }
            if (kinds == null) {
                return Optional.empty();
            }
            final ImmutableSet<IPrimitiveType.Kind> allowedKinds = kinds;
            return DEFAULT_KINDS.stream()
                    .filter(allowedKinds::contains)
                    .findFirst()
                    .<Term>map(kind -> new Known(new IPrimitiveType(kind, isNullable)));
        }
    }
}
