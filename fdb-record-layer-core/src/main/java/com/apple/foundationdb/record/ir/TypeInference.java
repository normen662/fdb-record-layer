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
 * Infers the types of one statement: the type of every literal in it, the type arguments of every reference to a
 * generic function in it, and the types that its closures leave out.
 *
 * <p>A type that is not known yet is an inference variable. A statement requires types to be equal: the type of an
 * argument and the type of its parameter, the type stated for a variable and the type of its expression. FRL-IR has
 * neither subtypes nor promotions, so every requirement is an equation, and {@link #unify} solves the equations as they
 * are added; their order does not matter. The variable of a literal can only become a type that the literal can denote.
 * Once the statement has added all of its equations, {@link #solve()} gives every literal whose type is still open the
 * default type of the literal, and {@link #resolve} turns the terms of the statement into types.</p>
 *
 * <p>The unit of inference is a statement outside closures, together with every closure in it: the statements of a
 * closure join the inference of the statement that the closure is in, because the types of the closure's parameters
 * can come from anywhere in that statement, as in {@code apply({ x in x }, 1)}. Every other statement, including every
 * statement of a function body, is solved on its own: what it declares has a type when the statement is complete, and
 * later statements do not change it. So in a function body {@code let a = 42} makes {@code a} an int, while in a
 * closure a later {@code let b: bigint = a} can still make it a bigint.</p>
 *
 * <p>Nullability is a property of a type rather than a wrapper around it, so a type parameter {@code T} that occurs as
 * {@code T?} stands for its type made nullable. Matching {@code T?} with {@code int?} makes {@code T} an {@code int},
 * unless {@code T} must be nullable itself.</p>
 */
final class TypeInference {

    /**
     * The order in which a literal picks its default type among the types it can denote.
     */
    @Nonnull
    private static final ImmutableList<IPrimitiveType.Kind> DEFAULT_KINDS = ImmutableList.of(IPrimitiveType.Kind.INT,
            IPrimitiveType.Kind.DOUBLE, IPrimitiveType.Kind.STRING, IPrimitiveType.Kind.BOOLEAN);

    /**
     * The inference variables of the statement, in the order they were created.
     */
    @Nonnull
    private final List<Variable> variables = new ArrayList<>();

    /**
     * Returns a term for a type that is known. Such a term belongs to no inference, so any inference can use it.
     *
     * @param type the type
     * @return a term without inference variables
     */
    @Nonnull
    static Term known(@Nonnull final IType type) {
        return new Known(type);
    }

    /**
     * Returns whether a term stands for a type that is known, i.e. whether it contains no open inference variable.
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
     * Returns a term for the type of a literal, a new inference variable that can only become a type that the literal
     * can denote.
     *
     * @param kind the kind of the literal
     * @return a term for the type of the literal
     */
    @Nonnull
    Term literal(@Nonnull final LiteralKind kind) {
        return new VariableTerm(newVariable(kind.constraint), false);
    }

    /**
     * Returns a term for a type that is not known yet, such as the result type of a call, a new inference variable.
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
     * Returns the type of a reference to a function, in which a new inference variable stands for each type parameter
     * of the function. The variables become the type arguments of the reference.
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
     * Requires two terms to stand for the same type.
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
     * Gives every inference variable that is still open and belongs to a literal the default type of the literal. The
     * statement calls this after it added all of its equations, and before it resolves its terms.
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
     * Returns the type that a term stands for, once {@link #solve()} has run for the inference that the term's variables
     * belong to.
     *
     * @param term the term
     * @return the type of the term
     * @throws IllegalArgumentException if the statement does not determine the type, as for {@code let x = nil}
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
     * Returns the term that a term stands for: a bound variable stands for what it is bound to, made nullable where it
     * occurs as {@code T?}.
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
     * A type in which inference variables can stand for the parts that are not known yet.
     */
    interface Term {
    }

    /**
     * The type of a reference to a function, with an inference variable for each type parameter of the function.
     *
     * @param type the type of the reference
     * @param typeArguments the variables for the type parameters of the function, in order
     */
    record Instantiation(@Nonnull Term type, @Nonnull ImmutableList<Term> typeArguments) {
    }

    /**
     * A type without inference variables.
     */
    private record Known(@Nonnull IType type) implements Term {
    }

    /**
     * An inference variable, which stands for its type made nullable if {@code isNullable}, as {@code T?} does.
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
     * An inference variable: a type that is not known yet, until the variable is bound. The constraint restricts the
     * terms it can be bound to.
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
