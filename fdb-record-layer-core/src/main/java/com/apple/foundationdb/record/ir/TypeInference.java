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
import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IRecordType;
import com.apple.foundationdb.record.ir.fragments.IStreamType;
import com.apple.foundationdb.record.ir.fragments.IType;
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
 *     <li>the type of each literal;</li>
 *     <li>the type arguments of each reference to a generic function;</li>
 *     <li>the parameter types and the result type of each closure that does not state them.</li>
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
 *     <li>The visitor calls {@link #solve()}, which gives a default type to each open variable with a literal
 *     constraint.</li>
 *     <li>The builders of the fragments call {@link #resolve}, which converts each term to an {@link IType}.</li>
 * </ol>
 *
 * <p>Example: the third line of this program states no type.</p>
 * <pre>{@code
 * extern func apply<T, U>(f: (T) -> U, x: T): U
 * extern func h(y: bigint): string
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
     * The inference variables that this inference made, in the order of their creation. {@link #solve()} examines
     * them.
     */
    @Nonnull
    private final List<Variable> variables = new ArrayList<>();

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
     * <p>Example: for {@code apply<T, U>(f: (T) -> U, x: T): U}, the method makes {@code vT} and {@code vU}. It returns
     * the term {@code ((vT) -> vU, vT) -> vU} and the type arguments {@code vT} and {@code vU}. The two places of
     * {@code T} hold two different {@code VariableTerm} records, but both refer to {@code vT}. So one binding of
     * {@code vT} changes both places.</p>
     *
     * @param typeParameters the type parameters of the function
     * @param type the type of the function, in terms of its type parameters
     * @return the type of the reference and the type arguments
     */
    @Nonnull
    Instantiation instantiate(@Nonnull final List<IGenericType> typeParameters, @Nonnull final IFunctionType type) {
        final ImmutableMap.Builder<String, Variable> substitution = ImmutableMap.builder();
        final ImmutableList.Builder<Term> typeArguments = ImmutableList.builder();
        for (final IGenericType typeParameter : typeParameters) {
            final Variable variable = newVariable(Constraint.ANY);
            substitution.put(typeParameter.name(), variable);
            typeArguments.add(new VariableTerm(variable, false));
        }
        return new Instantiation(toTerm(type, substitution.build()), typeArguments.build());
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
     * Gives a default type to each open variable whose constraint has a default type, see {@link #DEFAULT_KINDS}. Only
     * the constraints of number, string, and boolean literals have one. The visitor calls this method after the
     * statement stated all of its equations, and before the builders resolve the terms. The method can run more than
     * one time, and each call changes only the variables that are open at that time. A for loop with its own inference
     * uses this, because it must know the type of its variable before it infers its body.
     *
     * <p>Example: in {@code let x = 42}, no equation fixes the variable of {@code 42}, so this method binds it to
     * {@code int}. In {@code let x = nil}, the variable of {@code nil} has no default type. It stays open, and
     * {@link #resolve} fails.</p>
     */
    void solve() {
        for (final Variable variable : variables) {
            final Optional<IType> defaultType = variable.constraint.defaultType();
            if (variable.binding == null && defaultType.isPresent()) {
                variable.binding = new Known(defaultType.get());
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
        // an open variable: no equation of the statement determines it, and it has no default
        throw new IllegalArgumentException("the type cannot be inferred");
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
        variable.binding = value;
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
     */
    private static boolean bindToVariable(@Nonnull final Variable variable, @Nonnull final VariableTerm target) {
        final Constraint moved = target.isNullable() ? variable.constraint.withoutNullability() : variable.constraint;
        if (!constrain(target.variable(), moved)) {
            return false;
        }
        variable.binding = target;
        return true;
    }

    /**
     * Adds a constraint to a variable, if a type can meet both the new and the existing one.
     */
    private static boolean constrain(@Nonnull final Variable variable, @Nonnull final Constraint constraint) {
        final Optional<Constraint> combined = variable.constraint.and(constraint);
        if (combined.isEmpty()) {
            return false;
        }
        variable.constraint = combined.get();
        return true;
    }

    /**
     * Returns whether a constraint allows a term that is not a variable term.
     */
    private static boolean allows(@Nonnull final Constraint constraint, @Nonnull final Term term) {
        if (term instanceof Known known) {
            return constraint.allows(known.type());
        }
        // an array, record, function, or stream type: only the constraint of nil, which has no kinds, allows one
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
        // a stream type, which is never nullable
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
        // a stream type, which is never nullable
        return null;
    }

    /**
     * Returns whether a variable occurs in a term. A variable must not be bound to a term it occurs in, as in
     * {@code T = [T]}: that would be an infinite type.
     */
    private static boolean occurs(@Nonnull final Variable variable, @Nonnull final Term term) {
        final Term pruned = prune(term);
        if (pruned instanceof VariableTerm variableTerm) {
            return variableTerm.variable() == variable;
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
        throw new IllegalArgumentException("a stream type cannot be nullable");
    }

    /**
     * Returns a term of the structure of a known array, record, function, or stream type, whose parts are terms of their
     * own, so that it can be unified part by part with a term that contains variables. Other terms are returned as
     * they are.
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
            this.constraint = new Constraint(kinds, isNullable);
        }
    }

    /**
     * A type that can contain inference variables. An {@link IType} cannot contain them, so the fragments contain no
     * terms. The visitor gets terms from {@link TypeInference#known}, {@link TypeInference#unknown},
     * {@link TypeInference#literal}, {@link TypeInference#function}, {@link TypeInference#array},
     * {@link TypeInference#stream}, and {@link TypeInference#instantiate}. It gives the terms back to
     * {@link TypeInference}, and it does not look into them. Only {@link TypeInference} does that.
     *
     * <p>A {@code Known} term holds a type without variables. A {@code VariableTerm} refers to a variable. Each type
     * that contains other types has a compound term kind, with a term for each part: {@code ArrayTerm},
     * {@code RecordTerm}, {@code FunctionTerm}, and {@code StreamTerm}. A {@code VariableTerm} can be in each part
     * position. So a term can describe a type with unknown parts, for example {@code [vT]}, an array of an unknown
     * element type.</p>
     *
     * <p>All terms are immutable records. But a {@code VariableTerm} refers to a {@link Variable}, which is mutable. So
     * the type that a term stands for changes when {@link TypeInference#unify} binds a variable in it. Example: after
     * {@code vT := int}, the term {@code [vT]} stands for {@code [int]}, and the term itself is unchanged.</p>
     */
    interface Term {
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
     * are no kinds, and a nullable type if {@code isNullable}.
     *
     * @param kinds the kinds of primitive types allowed, or {@code null} for any type
     * @param isNullable whether the type must be nullable
     */
    private record Constraint(@Nullable ImmutableSet<IPrimitiveType.Kind> kinds, boolean isNullable) {
        @Nonnull
        private static final Constraint ANY = new Constraint(null, false);
        @Nonnull
        private static final Constraint NULLABLE = new Constraint(null, true);

        private boolean allows(@Nonnull final IType type) {
            return (!isNullable || type.isNullable())
                    && (kinds == null || (type instanceof IPrimitiveType primitiveType && kinds.contains(primitiveType.kind())));
        }

        /**
         * Returns the constraint of a type that meets both this constraint and the other one, or empty if no type does.
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
            return Optional.of(new Constraint(commonKinds, isNullable || other.isNullable));
        }

        @Nonnull
        private Constraint withoutNullability() {
            return new Constraint(kinds, false);
        }

        /**
         * Returns the type of a literal that no equation determines: the first allowed kind in {@link #DEFAULT_KINDS}.
         */
        @Nonnull
        private Optional<IType> defaultType() {
            if (kinds == null) {
                return Optional.empty();
            }
            final ImmutableSet<IPrimitiveType.Kind> allowedKinds = kinds;
            return DEFAULT_KINDS.stream()
                    .filter(allowedKinds::contains)
                    .findFirst()
                    .map(kind -> new IPrimitiveType(kind, isNullable));
        }
    }
}
