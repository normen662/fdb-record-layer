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

package com.apple.foundationdb.record.ir.fragments;

import com.apple.foundationdb.half.Half;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the contract of {@link IFragment}: fragments with equal parts are equal and have the same stable hash code,
 * fragments that differ in one part are not equal, and the constructors reject fragments that are not well-formed.
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
                pinned(IFragmentTest::identityFunction, 1074525709),
                pinned(() -> new IProgram(ImmutableList.of(identityFunction())), 73444329),
                pinned(() -> new IFunctionDeclaration("lookup", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("key", bigintType())),
                        stringType().withNullability(true)), -227215891),
                pinned(() -> new IClosure.Parameter("x", intType()), 1044350755),
                pinned(() -> new IClosure.Parameter("y", bigintType()), 926581826),
                pinned(() -> new IClosure(ImmutableList.of(new IClosure.Parameter("x", intType()),
                        new IClosure.Parameter("y", bigintType())), voidType(), emptyBlock()), 1543548840),
                pinned(IFragmentTest::literal42, 1198197660),
                pinned(() -> new ILiteral(42L, bigintType()), 1080428700),
                // the hash code of a half is its 16-bit pattern, 0x3E00 for 1.5
                pinned(() -> new ILiteral(Half.valueOf(1.5f), new IPrimitiveType(IPrimitiveType.Kind.HALF, false)),
                        1294493890),
                pinned(() -> new ILiteral(null, stringType().withNullability(true)), -686800226),
                pinned(() -> new IVariableReference("x", intType()), -73490170),
                pinned(IFragmentTest::identityReference, -777129065),
                pinned(() -> new IFunctionCall(identityReference(), ImmutableList.of(literal42())), 179555209),
                pinned(() -> new IAssignment("x", literal42()), -405760496));
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
        final IFunctionDeclaration declaration = new IFunctionDeclaration("lookup", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinition.Parameter("key", bigintType)), intType);
        final IClosure.Parameter closureX = new IClosure.Parameter("x", intType);
        final IClosure closure = new IClosure(ImmutableList.of(closureX), intType,
                new IBlock(ImmutableList.of(new IVariableReference("x", intType))));
        final ILiteral literal = literal42();
        final IFunctionType intToInt = new IFunctionType(false, ImmutableList.of(intType), intType);
        final IFunctionReference reference = identityReference();
        final IFunctionCall call = new IFunctionCall(reference, ImmutableList.of(literal));
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
                        voidType(), id.body())),
                Arguments.of(id, new IFunctionDefinition("id", id.typeParameters(), id.parameters(), id.resultType(),
                        new IBlock(ImmutableList.of(new IVariableReference("y", t))))),
                // parameters of function definitions: the name, the type
                Arguments.of(x, new IFunctionDefinition.Parameter("y", t)),
                Arguments.of(x, new IFunctionDefinition.Parameter("x", intType)),
                // function declarations: the name, the type parameters, the parameters, the result type
                Arguments.of(declaration, new IFunctionDeclaration("find", declaration.typeParameters(),
                        declaration.parameters(), declaration.resultType())),
                Arguments.of(declaration, new IFunctionDeclaration("lookup", ImmutableList.of(t),
                        declaration.parameters(), declaration.resultType())),
                Arguments.of(declaration, new IFunctionDeclaration("lookup", declaration.typeParameters(),
                        ImmutableList.of(), declaration.resultType())),
                Arguments.of(declaration, new IFunctionDeclaration("lookup", declaration.typeParameters(),
                        declaration.parameters(), voidType())),
                // closures: the parameters, the result type, the body
                Arguments.of(closure, new IClosure(ImmutableList.of(), closure.resultType(), closure.body())),
                Arguments.of(closure, new IClosure(closure.parameters(), voidType(), closure.body())),
                Arguments.of(closure, new IClosure(closure.parameters(), closure.resultType(),
                        new IBlock(ImmutableList.of(literal42())))),
                // parameters of closures: the name, the type
                Arguments.of(closureX, new IClosure.Parameter("y", intType)),
                Arguments.of(closureX, new IClosure.Parameter("x", bigintType)),
                // literals: the value, the type
                Arguments.of(literal, new ILiteral(43, intType)),
                Arguments.of(literal, new ILiteral(42, intType.withNullability(true))),
                Arguments.of(new ILiteral(null, intType.withNullability(true)),
                        new ILiteral(null, bigintType.withNullability(true))),
                // references to variables: the name, the type
                Arguments.of(new IVariableReference("x", intType), new IVariableReference("y", intType)),
                Arguments.of(new IVariableReference("x", intType), new IVariableReference("x", bigintType)),
                // references to functions: the name, the type arguments, the type
                Arguments.of(reference, new IFunctionReference("f", reference.typeArguments(), reference.type())),
                Arguments.of(reference, new IFunctionReference("id", ImmutableList.of(), reference.type())),
                Arguments.of(reference, new IFunctionReference("id", reference.typeArguments(),
                        new IFunctionType(false, ImmutableList.of(intType), intType.withNullability(true)))),
                // calls: the callee, the arguments
                Arguments.of(call, new IFunctionCall(new IVariableReference("id", intToInt), call.arguments())),
                Arguments.of(call, new IFunctionCall(reference, ImmutableList.of(new ILiteral(43, intType)))),
                // assignments: the name, the expression
                Arguments.of(new IAssignment("x", literal), new IAssignment("y", literal)),
                Arguments.of(new IAssignment("x", literal), new IAssignment("x", call)),
                // fragments of different kinds whose parts are equal
                Arguments.of(emptyBlock, new IProgram(ImmutableList.of())),
                Arguments.of(new IFunctionDefinition.Parameter("x", intType), closureX),
                Arguments.of(new IVariableReference("id", intToInt), new IFunctionReference("id", ImmutableList.of(),
                        intToInt)),
                Arguments.of(new IFunctionDeclaration("g", ImmutableList.of(), ImmutableList.of(), voidType()),
                        new IFunctionDefinition("g", ImmutableList.of(), ImmutableList.of(), voidType(), emptyBlock)));
    }

    @ParameterizedTest
    @MethodSource("fragmentsDifferingInOnePart")
    void equalsDifferentPartIsFalse(@Nonnull final IFragment fragment, @Nonnull final IFragment other) {
        assertThat(fragment).isNotEqualTo(other);
        assertThat(other).isNotEqualTo(fragment);
    }

    @Test
    void valueLastStatementIsExpressionReturnsIt() {
        final IAssignment assignment = new IAssignment("x", literal42());
        assertThat(new IBlock(ImmutableList.of(assignment, literal42())).value()).contains(literal42());
        // a block whose last statement is not an expression, or that has no statements, has no value
        assertThat(new IBlock(ImmutableList.of(literal42(), assignment)).value()).isEmpty();
        assertThat(emptyBlock().value()).isEmpty();
    }

    @Nonnull
    static Stream<Arguments> bodiesAndResultTypes() {
        final IBlock valueIsLiteral = new IBlock(ImmutableList.of(literal42()));
        return Stream.of(
                // a value of the result type
                Arguments.of(valueIsLiteral, intType(), true),
                Arguments.of(new IBlock(ImmutableList.of(new IAssignment("x", literal42()), literal42())), intType(),
                        true),
                // no value, or a value of another type: there is no promotion
                Arguments.of(emptyBlock(), intType(), false),
                Arguments.of(new IBlock(ImmutableList.of(new IAssignment("x", literal42()))), intType(), false),
                Arguments.of(valueIsLiteral, bigintType(), false),
                Arguments.of(valueIsLiteral, intType().withNullability(true), false),
                // a void function discards the value of its body, if there is one
                Arguments.of(valueIsLiteral, voidType(), true),
                Arguments.of(emptyBlock(), voidType(), true));
    }

    @ParameterizedTest
    @MethodSource("bodiesAndResultTypes")
    void isBodyOfResultTypeMatchesValueOrIsVoid(@Nonnull final IBlock body, @Nonnull final IType resultType,
                                                final boolean expected) {
        assertThat(body.isBodyOf(resultType)).isEqualTo(expected);
    }

    @Test
    void constructorIllFormedFragmentFails() {
        // a body must end with a value of the result type, unless that is void
        assertThatThrownBy(() -> new IFunctionDefinition("f", ImmutableList.of(), ImmutableList.of(), intType(),
                emptyBlock())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IClosure(ImmutableList.of(), intType(),
                new IBlock(ImmutableList.of(new ILiteral(42L, bigintType()))))).isInstanceOf(IllegalArgumentException.class);
        // void is only a result type, and it is never nullable
        assertThatThrownBy(() -> new IPrimitiveType(IPrimitiveType.Kind.VOID, true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> voidType().withNullability(true)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IArrayType(false, voidType())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IStreamType(voidType())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> IRecordType.Field.unnamedOf(voidType())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IFunctionType(false, ImmutableList.of(voidType()), intType()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IFunctionDefinition.Parameter("x", voidType()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IClosure.Parameter("x", voidType())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IAssignment("x", new IFunctionCall(new IVariableReference("g",
                new IFunctionType(false, ImmutableList.of(), voidType())), ImmutableList.of())))
                .isInstanceOf(IllegalArgumentException.class);
        // the parameters of a declared function have distinct names
        assertThatThrownBy(() -> new IFunctionDeclaration("f", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinition.Parameter("x", intType()),
                        new IFunctionDefinition.Parameter("x", intType())), voidType()))
                .isInstanceOf(IllegalArgumentException.class);
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
    private static IPrimitiveType voidType() {
        return new IPrimitiveType(IPrimitiveType.Kind.VOID, false);
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
     * Returns {@code func id<T>(x: T): T { x }}.
     */
    @Nonnull
    private static IFunctionDefinition identityFunction() {
        return new IFunctionDefinition("id", ImmutableList.of(typeParameter("T")),
                ImmutableList.of(new IFunctionDefinition.Parameter("x", typeParameter("T"))), typeParameter("T"),
                new IBlock(ImmutableList.of(new IVariableReference("x", typeParameter("T")))));
    }

    /**
     * Returns the reference to {@code id} in {@code id(42)}, with the type argument {@code int}.
     */
    @Nonnull
    private static IFunctionReference identityReference() {
        return new IFunctionReference("id", ImmutableList.of(intType()),
                new IFunctionType(false, ImmutableList.of(intType()), intType()));
    }

    @Nonnull
    private static ILiteral literal42() {
        return new ILiteral(42, intType());
    }
}
