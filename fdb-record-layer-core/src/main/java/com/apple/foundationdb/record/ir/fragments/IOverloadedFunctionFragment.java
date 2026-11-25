/*
 * IOverloadedFunctionFragment.java
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
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * An overloaded function, {@code overloaded func add<A, B>(a: A, b: B)}: a name whose variants are functions of their
 * own, one for each key of concrete type arguments. The declaration states what all variants have in common: the type
 * parameters and the parameters, with their labels. It has no body and no result type, because each variant has its
 * own implementation and its own result type, see {@link Variant}.
 *
 * <p>A call of an overloaded function calls one of its variants. The parser selects the variant from the types of the
 * arguments and the type that the context of the call requires, so a reference to the function, an
 * {@link IFunctionReferenceFragment}, refers to the selected variant, not to this declaration.</p>
 */
@API(API.Status.EXPERIMENTAL)
public final class IOverloadedFunctionFragment implements IStatementFragment {
    @Nonnull
    private final String name;
    @Nonnull
    private final ImmutableList<IGenericType> typeParameters;
    @Nonnull
    private final ImmutableList<IFunctionDefinitionFragment.Parameter> parameters;
    @Nonnull
    private final ImmutableList<IAnnotationFragment> annotations;
    @Nonnull
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    /**
     * Creates an overloaded function.
     *
     * @param name the name of the function and of its variants
     * @param typeParameters the type parameters, in order; there is at least one, because the type arguments of a
     *        variant are its key
     * @param parameters the parameters of every variant, in order, with distinct names
     * @param annotations the annotations of the declaration, in order
     */
    public IOverloadedFunctionFragment(@Nonnull final String name, @Nonnull final List<IGenericType> typeParameters,
                                       @Nonnull final List<IFunctionDefinitionFragment.Parameter> parameters,
                                       @Nonnull final List<IAnnotationFragment> annotations) {
        this.name = Objects.requireNonNull(name);
        this.typeParameters = ImmutableList.copyOf(typeParameters);
        this.parameters = ImmutableList.copyOf(parameters);
        this.annotations = ImmutableList.copyOf(annotations);
        Preconditions.checkArgument(!this.typeParameters.isEmpty(), "an overloaded function must have type parameters");
        Preconditions.checkArgument(this.parameters.stream()
                        .map(IFunctionDefinitionFragment.Parameter::name)
                        .distinct()
                        .count() == this.parameters.size(),
                "the parameters of a function must have distinct names");
    }

    /**
     * {@return the name of the function and of its variants}
     */
    @Nonnull
    public String name() {
        return name;
    }

    /**
     * {@return the type parameters, in order}
     */
    @Nonnull
    public List<IGenericType> typeParameters() {
        return typeParameters;
    }

    /**
     * {@return the parameters of every variant, in order}
     */
    @Nonnull
    public List<IFunctionDefinitionFragment.Parameter> parameters() {
        return parameters;
    }

    /**
     * {@return the annotations of the declaration, in order}
     */
    @Nonnull
    public List<IAnnotationFragment> annotations() {
        return annotations;
    }

    /**
     * Returns the parameters of the variant with the given type arguments: the parameters of this function, with each
     * type parameter replaced by its type argument. A variant must have exactly these parameters.
     *
     * <p>Example: for {@code overloaded func add<A, B>(a: A, b: B?)}, the type arguments {@code int} and
     * {@code bigint} give the parameters {@code a: int} and {@code b: bigint?}.</p>
     *
     * @param typeArguments one type for each type parameter, in order
     * @return the parameters of the variant, in order
     */
    @Nonnull
    public List<IFunctionDefinitionFragment.Parameter> parametersOfVariant(@Nonnull final List<IType> typeArguments) {
        Preconditions.checkArgument(typeArguments.size() == typeParameters.size(),
                "a variant has one type argument for each type parameter of its overloaded function");
        final ImmutableMap.Builder<String, IType> substitution = ImmutableMap.builder();
        for (int i = 0; i < typeParameters.size(); i++) {
            substitution.put(typeParameters.get(i).name(), typeArguments.get(i));
        }
        final Map<String, IType> typeArgumentsByName = substitution.build();
        return parameters.stream()
                .map(parameter -> new IFunctionDefinitionFragment.Parameter(parameter.label().orElse(null),
                        parameter.name(), substitute(parameter.type(), typeArgumentsByName)))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Returns a type with each type parameter in it replaced by its type argument. A type parameter that occurs as
     * {@code T?} becomes its type argument made nullable, as in the inference of the parser: for {@code T := int?}, it
     * is {@code int?}.
     */
    @Nonnull
    private static IType substitute(@Nonnull final IType type, @Nonnull final Map<String, IType> typeArguments) {
        if (type instanceof IGenericType genericType) {
            final IType typeArgument = typeArguments.get(genericType.name());
            if (typeArgument == null) {
                // a type parameter of a function that the overloaded function is nested in
                return type;
            }
            return genericType.isNullable() ? typeArgument.withNullability(true) : typeArgument;
        }
        if (type instanceof IArrayType arrayType) {
            return new IArrayType(arrayType.isNullable(), substitute(arrayType.elementType(), typeArguments));
        }
        if (type instanceof IRecordType recordType) {
            return new IRecordType(recordType.isNullable(), recordType.fields()
                    .stream()
                    .map(field -> new IRecordType.Field(substitute(field.type(), typeArguments), field.name(),
                            field.number()))
                    .collect(ImmutableList.toImmutableList()));
        }
        if (type instanceof IFunctionType functionType) {
            return new IFunctionType(functionType.isNullable(), functionType.parameterTypes()
                    .stream()
                    .map(parameterType -> substitute(parameterType, typeArguments))
                    .collect(ImmutableList.toImmutableList()), substitute(functionType.resultType(), typeArguments));
        }
        if (type instanceof IStreamType streamType) {
            return new IStreamType(substitute(streamType.elementType(), typeArguments));
        }
        if (type instanceof IMetaType metaType) {
            return new IMetaType(substitute(metaType.type(), typeArguments));
        }
        return type;
    }

    /**
     * Returns whether a type contains a type parameter.
     */
    private static boolean hasTypeParameter(@Nonnull final IType type) {
        if (type instanceof IGenericType) {
            return true;
        }
        if (type instanceof IArrayType arrayType) {
            return hasTypeParameter(arrayType.elementType());
        }
        if (type instanceof IRecordType recordType) {
            return recordType.fields().stream().anyMatch(field -> hasTypeParameter(field.type()));
        }
        if (type instanceof IFunctionType functionType) {
            return functionType.parameterTypes().stream().anyMatch(IOverloadedFunctionFragment::hasTypeParameter)
                    || hasTypeParameter(functionType.resultType());
        }
        if (type instanceof IStreamType streamType) {
            return hasTypeParameter(streamType.elementType());
        }
        if (type instanceof IMetaType metaType) {
            return hasTypeParameter(metaType.type());
        }
        return false;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof IOverloadedFunctionFragment that && name.equals(that.name)
                && typeParameters.equals(that.typeParameters) && parameters.equals(that.parameters)
                && annotations.equals(that.annotations);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Objects.hash("overloaded function", name, typeParameters, parameters, annotations);
    }

    @Nonnull
    @Override
    public String toString() {
        return "IOverloadedFunctionFragment[name=" + name + ", typeParameters=" + typeParameters + ", parameters="
                + parameters + ", annotations=" + annotations + "]";
    }

    /**
     * What makes a function a variant of an overloaded function: the overloaded function, and the type arguments that
     * select the variant, its key. A variant is an {@link IFunctionDefinitionFragment} or an
     * {@link IFunctionDeclarationFragment} that holds such a part, see {@link IFunctionFragment#variant()}. The type
     * arguments are concrete types: they contain no type parameters, and none of them is {@code void}. The function
     * has no type parameters of its own, and its parameters are those of the overloaded function for its type
     * arguments, see {@link IOverloadedFunctionFragment#parametersOfVariant}. The result type and the implementation
     * belong to the function.
     *
     * <p>Example: {@code variant func add<int, bigint>(a: int, b: bigint) -> bigint { … }} is the variant with the
     * key {@code <int, bigint>} of {@code overloaded func add<A, B>(a: A, b: B)}.</p>
     */
    public static final class Variant implements IFragment {
        @Nonnull
        private final IOverloadedFunctionFragment overloadedFunction;
        @Nonnull
        private final ImmutableList<IType> typeArguments;
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        /**
         * Creates the variant part of a function.
         *
         * @param overloadedFunction the overloaded function that the function is a variant of
         * @param typeArguments the key of the variant: one concrete type for each type parameter of the overloaded
         *        function, in order
         */
        public Variant(@Nonnull final IOverloadedFunctionFragment overloadedFunction,
                       @Nonnull final List<IType> typeArguments) {
            this.overloadedFunction = Objects.requireNonNull(overloadedFunction);
            this.typeArguments = ImmutableList.copyOf(typeArguments);
            Preconditions.checkArgument(this.typeArguments.size() == overloadedFunction.typeParameters().size(),
                    "a variant has one type argument for each type parameter of its overloaded function");
            Preconditions.checkArgument(this.typeArguments.stream()
                            .noneMatch(typeArgument -> typeArgument.isVoid() || hasTypeParameter(typeArgument)),
                    "the type arguments of a variant are concrete types that are not void");
        }

        /**
         * {@return the overloaded function that the function is a variant of}
         */
        @Nonnull
        public IOverloadedFunctionFragment overloadedFunction() {
            return overloadedFunction;
        }

        /**
         * {@return the key of the variant: one concrete type for each type parameter of the overloaded function}
         */
        @Nonnull
        public List<IType> typeArguments() {
            return typeArguments;
        }

        /**
         * Checks that a function with the given signature can be this variant. The function must have the name of
         * the overloaded function, no type parameters of its own, and the parameters of the overloaded function for
         * the type arguments of this variant, with the same names and labels.
         *
         * @param name the name of the function
         * @param typeParameters the type parameters of the function
         * @param parameters the parameters of the function, in order
         */
        void checkSignature(@Nonnull final String name, @Nonnull final List<IGenericType> typeParameters,
                            @Nonnull final List<IFunctionDefinitionFragment.Parameter> parameters) {
            Preconditions.checkArgument(name.equals(overloadedFunction.name()) && typeParameters.isEmpty(),
                    "a variant has the name of its overloaded function and no type parameters of its own");
            Preconditions.checkArgument(parameters.equals(overloadedFunction.parametersOfVariant(typeArguments)),
                    "a variant has the parameters of its overloaded function, with its type arguments in place of the "
                            + "type parameters");
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            return other instanceof Variant that && overloadedFunction.equals(that.overloadedFunction)
                    && typeArguments.equals(that.typeArguments);
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        private int computeHashCode() {
            return Objects.hash("variant", overloadedFunction, typeArguments);
        }

        @Nonnull
        @Override
        public String toString() {
            return "Variant[overloadedFunction=" + overloadedFunction.name() + ", typeArguments=" + typeArguments + "]";
        }
    }
}
