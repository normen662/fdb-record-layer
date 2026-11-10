/*
 * IFragmentTest.java
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

package com.apple.foundationdb.record.ir.fragment;

import com.apple.foundationdb.record.ir.program.IBlock;
import com.apple.foundationdb.record.ir.program.IClosure;
import com.apple.foundationdb.record.ir.program.IFunctionDefinition;
import com.apple.foundationdb.record.ir.program.IProgram;
import com.apple.foundationdb.record.ir.types.IArrayType;
import com.apple.foundationdb.record.ir.types.IEnumType;
import com.apple.foundationdb.record.ir.types.IFunctionType;
import com.apple.foundationdb.record.ir.types.IGenericType;
import com.apple.foundationdb.record.ir.types.IPrimitiveType;
import com.apple.foundationdb.record.ir.types.IRecordType;
import com.apple.foundationdb.record.ir.types.IStreamType;
import com.apple.foundationdb.record.ir.types.IType;
import com.apple.foundationdb.record.ir.types.IVectorType;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the contract of {@link IFragment}: fragments with equal parts are equal and have the same stable hash code,
 * and fragments that differ in one part are not equal.
 */
class IFragmentTest {

    /**
     * Returns one fragment of every kind, a copy of it, and its hash code. The hash codes were computed from the rule
     * that {@link IFragment#hashCode()} states, not from the implementation. They are the same in every run of every
     * JVM, so a failure means that the rule or its implementation changed.
     */
    @Nonnull
    static Stream<Arguments> fragmentsWithHashCodes() {
        return Stream.of(
                pinned(IFragmentTest::intType, 1947197752),
                pinned(() -> stringType().withNullability(true), 62201168),
                pinned(() -> typeParameter("T"), 888085123),
                pinned(() -> IRecordType.Field.of(bigintType(), "id", 1), -215616676),
                pinned(() -> new IRecordType(true, ImmutableList.of(IRecordType.Field.of(bigintType(), "id", 1),
                        IRecordType.Field.unnamedOf(stringType().withNullability(true)))), -1114434836),
                pinned(() -> new IArrayType(true, intType().withNullability(true)), 635429635),
                pinned(() -> new IVectorType(false, IPrimitiveType.Kind.HALF, 3), 1759803887),
                pinned(() -> new IEnumType.Value("RED", 0), 312227096),
                pinned(() -> new IEnumType(false,
                        ImmutableList.of(new IEnumType.Value("RED", 0), new IEnumType.Value("GREEN", 1))), -290165389),
                pinned(() -> new IFunctionType(false, ImmutableList.of(intType(), typeParameter("T")),
                        typeParameter("T").withNullability(true)), -2116708027),
                pinned(() -> new IStreamType(intType()), -943587233),
                pinned(() -> new IFunctionDefinition.Parameter("x", typeParameter("T")), -853251890),
                pinned(IFragmentTest::emptyBlock, -1386164011),
                pinned(IFragmentTest::identityFunction, -2087838818),
                pinned(() -> new IProgram(ImmutableList.of(identityFunction())), 1206047098),
                pinned(() -> new IClosure.Parameter("x", Optional.of(intType())), 1044350755),
                pinned(() -> new IClosure.Parameter("y", Optional.empty()), -902846966),
                pinned(() -> new IClosure(ImmutableList.of(new IClosure.Parameter("x", Optional.of(intType())),
                        new IClosure.Parameter("y", Optional.empty())), Optional.of(bigintType()), emptyBlock()),
                        1659584011));
    }

    @ParameterizedTest
    @MethodSource("fragmentsWithHashCodes")
    void equalsAndHashCodeCopiesAreEqualWithPinnedHashCode(@Nonnull final IFragment fragment,
                                                           @Nonnull final IFragment copy,
                                                           final int expectedHashCode) {
        assertThat(copy).isNotSameAs(fragment).isEqualTo(fragment);
        assertThat(fragment).isEqualTo(copy);
        assertThat(fragment.hashCode()).isEqualTo(expectedHashCode);
        assertThat(copy.hashCode()).isEqualTo(expectedHashCode);
    }

    @Nonnull
    static Stream<Arguments> fragmentsDifferingInOnePart() {
        final IType intType = intType();
        final IType bigintType = bigintType();
        final IGenericType t = typeParameter("T");
        final IBlock emptyBlock = emptyBlock();
        final IBlock nonEmptyBlock = new IBlock(ImmutableList.of(identityFunction()));
        final IRecordType.Field a = IRecordType.Field.of(intType, "a");
        final IRecordType.Field b = IRecordType.Field.of(bigintType, "b");
        final IVectorType vector = new IVectorType(false, IPrimitiveType.Kind.FLOAT, 3);
        final IEnumType.Value red = new IEnumType.Value("RED", 0);
        final IEnumType.Value green = new IEnumType.Value("GREEN", 1);
        final IFunctionType function = new IFunctionType(false, ImmutableList.of(intType), bigintType);
        final IFunctionDefinition id = identityFunction();
        final IFunctionDefinition.Parameter x = new IFunctionDefinition.Parameter("x", t);
        final IClosure.Parameter closureX = new IClosure.Parameter("x", Optional.of(intType));
        final IClosure closure = new IClosure(ImmutableList.of(closureX), Optional.of(bigintType), emptyBlock);
        return Stream.of(
                // primitive types: the kind, the nullability
                Arguments.of(intType, bigintType),
                Arguments.of(intType, intType.withNullability(true)),
                // record types: the nullability, the fields and their order
                Arguments.of(new IRecordType(false, ImmutableList.of(a)), new IRecordType(true, ImmutableList.of(a))),
                Arguments.of(new IRecordType(false, ImmutableList.of(a, b)),
                        new IRecordType(false, ImmutableList.of(b, a))),
                Arguments.of(new IRecordType(false, ImmutableList.of(a)),
                        new IRecordType(false, ImmutableList.of(a, b))),
                // fields: the type, the name, the number
                Arguments.of(a, IRecordType.Field.of(bigintType, "a")),
                Arguments.of(a, IRecordType.Field.of(intType, "b")),
                Arguments.of(a, IRecordType.Field.unnamedOf(intType)),
                Arguments.of(a, IRecordType.Field.of(intType, "a", 1)),
                Arguments.of(IRecordType.Field.of(intType, "a", 1), IRecordType.Field.of(intType, "a", 2)),
                // array types: the nullability, the element type
                Arguments.of(new IArrayType(false, intType), new IArrayType(true, intType)),
                Arguments.of(new IArrayType(false, intType), new IArrayType(false, bigintType)),
                // vector types: the nullability, the element type, the dimensions
                Arguments.of(vector, vector.withNullability(true)),
                Arguments.of(vector, new IVectorType(false, IPrimitiveType.Kind.DOUBLE, 3)),
                Arguments.of(vector, new IVectorType(false, IPrimitiveType.Kind.FLOAT, 4)),
                // enum types: the nullability, the values and their order
                Arguments.of(new IEnumType(false, ImmutableList.of(red, green)),
                        new IEnumType(true, ImmutableList.of(red, green))),
                Arguments.of(new IEnumType(false, ImmutableList.of(red, green)),
                        new IEnumType(false, ImmutableList.of(green, red))),
                // enum values: the name, the number
                Arguments.of(red, new IEnumType.Value("GREEN", 0)),
                Arguments.of(red, new IEnumType.Value("RED", 1)),
                // function types: the nullability, the parameter types, the result type
                Arguments.of(function, function.withNullability(true)),
                Arguments.of(function, new IFunctionType(false, ImmutableList.of(bigintType), bigintType)),
                Arguments.of(function, new IFunctionType(false, ImmutableList.of(intType), intType)),
                // stream types: the element type
                Arguments.of(new IStreamType(intType), new IStreamType(bigintType)),
                // generic types: the name, the nullability
                Arguments.of(t, typeParameter("U")),
                Arguments.of(t, t.withNullability(true)),
                // blocks and programs: the statements
                Arguments.of(emptyBlock, nonEmptyBlock),
                Arguments.of(new IProgram(ImmutableList.of()), new IProgram(nonEmptyBlock.statements())),
                // function definitions: the name, the type parameters, the parameters, the result type, the body
                Arguments.of(id, new IFunctionDefinition("f", id.typeParameters(), id.parameters(), id.resultType(),
                        id.body())),
                Arguments.of(id, new IFunctionDefinition("id", ImmutableList.of(t, typeParameter("U")),
                        id.parameters(), id.resultType(), id.body())),
                Arguments.of(id, new IFunctionDefinition("id", id.typeParameters(), ImmutableList.of(),
                        id.resultType(), id.body())),
                Arguments.of(id, new IFunctionDefinition("id", id.typeParameters(), id.parameters(),
                        t.withNullability(true), id.body())),
                Arguments.of(id, new IFunctionDefinition("id", id.typeParameters(), id.parameters(), id.resultType(),
                        nonEmptyBlock)),
                // parameters of function definitions: the name, the type
                Arguments.of(x, new IFunctionDefinition.Parameter("y", t)),
                Arguments.of(x, new IFunctionDefinition.Parameter("x", intType)),
                // closures: the parameters, the result type, the body
                Arguments.of(closure, new IClosure(ImmutableList.of(), closure.resultType(), closure.body())),
                Arguments.of(closure, new IClosure(closure.parameters(), Optional.empty(), closure.body())),
                Arguments.of(closure, new IClosure(closure.parameters(), closure.resultType(), nonEmptyBlock)),
                // parameters of closures: the name, the type
                Arguments.of(closureX, new IClosure.Parameter("y", Optional.of(intType))),
                Arguments.of(closureX, new IClosure.Parameter("x", Optional.empty())),
                // fragments of different kinds whose parts are equal
                Arguments.of(emptyBlock, new IProgram(ImmutableList.of())),
                Arguments.of(new IFunctionDefinition.Parameter("x", intType), closureX));
    }

    @ParameterizedTest
    @MethodSource("fragmentsDifferingInOnePart")
    void equalsDifferentPartIsFalse(@Nonnull final IFragment fragment, @Nonnull final IFragment other) {
        assertThat(fragment).isNotEqualTo(other);
        assertThat(other).isNotEqualTo(fragment);
    }

    /**
     * Returns the arguments of {@link #equalsAndHashCodeCopiesAreEqualWithPinnedHashCode}: two fragments that
     * {@code builder} builds one after the other, which are equal but share no parts, and the hash code they must have.
     */
    @Nonnull
    private static Arguments pinned(@Nonnull final Supplier<IFragment> builder, final int expectedHashCode) {
        return Arguments.of(builder.get(), builder.get(), expectedHashCode);
    }

    @Nonnull
    private static IPrimitiveType intType() {
        return new IPrimitiveType(IPrimitiveType.Kind.INT, false);
    }

    @Nonnull
    private static IPrimitiveType bigintType() {
        return new IPrimitiveType(IPrimitiveType.Kind.BIGINT, false);
    }

    @Nonnull
    private static IPrimitiveType stringType() {
        return new IPrimitiveType(IPrimitiveType.Kind.STRING, false);
    }

    @Nonnull
    private static IGenericType typeParameter(@Nonnull final String name) {
        return new IGenericType(name, false);
    }

    @Nonnull
    private static IBlock emptyBlock() {
        return new IBlock(ImmutableList.of());
    }

    /**
     * Returns {@code func id<T>(x: T): T { }}.
     */
    @Nonnull
    private static IFunctionDefinition identityFunction() {
        return new IFunctionDefinition("id", ImmutableList.of(typeParameter("T")),
                ImmutableList.of(new IFunctionDefinition.Parameter("x", typeParameter("T"))), typeParameter("T"),
                emptyBlock());
    }
}
