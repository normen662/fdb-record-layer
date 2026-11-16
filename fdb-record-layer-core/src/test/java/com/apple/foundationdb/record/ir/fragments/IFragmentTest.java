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
                pinned(IFragmentTest::identityFunction, -1049441388),
                pinned(() -> new IProgram(ImmutableList.of(identityFunction())), -2050522768),
                pinned(() -> new IFunctionDeclaration("lookup", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("key", bigintType())),
                        stringType().withNullability(true)), 688860876),
                pinned(() -> new IFunctionDeclaration("scan", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinition.Parameter("range",
                                new IExternType(rangeDeclaration(), false))),
                        intType(), ImmutableList.of(), abcPackage()), 1790714996),
                pinned(() -> new IAnnotationFragment.Argument(null, "com.apple.foundationdb.record.ir"), 1230968439),
                pinned(() -> new IAnnotationFragment.Argument("class", "com.apple.foundationdb.record.TupleRange"),
                        1319917643),
                pinned(IFragmentTest::javaPackageAnnotation, 1583677570),
                pinned(IFragmentTest::abcPackage, -1125828173),
                pinned(IFragmentTest::rangeDeclaration, 1798781256),
                pinned(() -> new IExternType(rangeDeclaration(), true), -1676417504),
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
                pinned(IFragmentTest::identityReference, 23787472),
                pinned(() -> new IFunctionCall(identityReference(), ImmutableList.of(literal42())), -761835920),
                pinned(() -> new IAssignment("x", literal42()), -405760496),
                pinned(() -> new IYieldFragment(literal42()), 467444080),
                pinned(() -> new IStreamFragment(intType(),
                        new IBlock(ImmutableList.of(new IYieldFragment(literal42())))), 1105828874));
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
        final IAnnotationFragment.Argument packageArgument =
                new IAnnotationFragment.Argument(null, "com.apple.foundationdb.record.ir");
        final IAnnotationFragment annotation = javaPackageAnnotation();
        final IPackageStatement abc = abcPackage();
        final ITypeDeclaration range = rangeDeclaration();
        final IExternType rangeType = new IExternType(range, false);
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
                // function definitions: the name, the type parameters, the parameters, the result type, the body, the
                // annotations
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
                Arguments.of(id, new IFunctionDefinition("id", id.typeParameters(), id.parameters(), id.resultType(),
                        id.body(), ImmutableList.of(annotation))),
                // parameters of function definitions: the name, the type
                Arguments.of(x, new IFunctionDefinition.Parameter("y", t)),
                Arguments.of(x, new IFunctionDefinition.Parameter("x", intType)),
                // function declarations: the name, the type parameters, the parameters, the result type, the
                // annotations, the package
                Arguments.of(declaration, new IFunctionDeclaration("find", declaration.typeParameters(),
                        declaration.parameters(), declaration.resultType())),
                Arguments.of(declaration, new IFunctionDeclaration("lookup", ImmutableList.of(t),
                        declaration.parameters(), declaration.resultType())),
                Arguments.of(declaration, new IFunctionDeclaration("lookup", declaration.typeParameters(),
                        ImmutableList.of(), declaration.resultType())),
                Arguments.of(declaration, new IFunctionDeclaration("lookup", declaration.typeParameters(),
                        declaration.parameters(), voidType())),
                Arguments.of(declaration, new IFunctionDeclaration("lookup", declaration.typeParameters(),
                        declaration.parameters(), declaration.resultType(), ImmutableList.of(annotation), null)),
                Arguments.of(declaration, new IFunctionDeclaration("lookup", declaration.typeParameters(),
                        declaration.parameters(), declaration.resultType(), ImmutableList.of(), abc)),
                // annotations: the name, the arguments; arguments: the label, the value
                Arguments.of(annotation, new IAnnotationFragment("swift", annotation.arguments())),
                Arguments.of(annotation, new IAnnotationFragment("java", ImmutableList.of())),
                Arguments.of(packageArgument, new IAnnotationFragment.Argument("package", packageArgument.value())),
                Arguments.of(packageArgument, new IAnnotationFragment.Argument(null, "com.apple.foundationdb")),
                // package statements: the name, the annotations
                Arguments.of(abc, new IPackageStatement("def", abc.annotations())),
                Arguments.of(abc, new IPackageStatement("abc", ImmutableList.of())),
                // type declarations: the name, the annotations, the package
                Arguments.of(range, new ITypeDeclaration("interval", range.annotations(), range.packageStatement())),
                Arguments.of(range, new ITypeDeclaration("range", ImmutableList.of(), range.packageStatement())),
                Arguments.of(range, new ITypeDeclaration("range", range.annotations(), null)),
                // extern types: the declaration, the nullability
                Arguments.of(rangeType, new IExternType(new ITypeDeclaration("interval", range.annotations(),
                        range.packageStatement()), false)),
                Arguments.of(rangeType, rangeType.withNullability(true)),
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
                // references to functions: the function, the type arguments, the type
                Arguments.of(reference, new IFunctionReference(new IFunctionDefinition("f", id.typeParameters(),
                        id.parameters(), id.resultType(), id.body()), reference.typeArguments(), reference.type())),
                Arguments.of(reference, new IFunctionReference(id, ImmutableList.of(bigintType), reference.type())),
                Arguments.of(reference, new IFunctionReference(id, reference.typeArguments(),
                        new IFunctionType(false, ImmutableList.of(intType), intType.withNullability(true)))),
                // calls: the callee, the arguments
                Arguments.of(call, new IFunctionCall(new IVariableReference("id", intToInt), call.arguments())),
                Arguments.of(call, new IFunctionCall(reference, ImmutableList.of(new ILiteral(43, intType)))),
                // assignments: the name, the expression
                Arguments.of(new IAssignment("x", literal), new IAssignment("y", literal)),
                Arguments.of(new IAssignment("x", literal), new IAssignment("x", call)),
                // yields: the value
                Arguments.of(new IYieldFragment(literal), new IYieldFragment(new ILiteral(43, intType))),
                // stream blocks: the element type, the block
                Arguments.of(new IStreamFragment(intType, emptyBlock), new IStreamFragment(bigintType, emptyBlock)),
                Arguments.of(new IStreamFragment(intType, emptyBlock),
                        new IStreamFragment(intType, new IBlock(ImmutableList.of(new IYieldFragment(literal))))),
                // fragments of different kinds whose parts are equal
                Arguments.of(emptyBlock, new IProgram(ImmutableList.of())),
                Arguments.of(new IFunctionDefinition.Parameter("x", intType), closureX),
                Arguments.of(new IVariableReference("id", intToInt), new IFunctionReference(new IFunctionDeclaration(
                        "id", ImmutableList.of(), ImmutableList.of(new IFunctionDefinition.Parameter("x", intType)),
                        intType), ImmutableList.of(), intToInt)),
                Arguments.of(new IFunctionDeclaration("g", ImmutableList.of(), ImmutableList.of(), voidType()),
                        new IFunctionDefinition("g", ImmutableList.of(), ImmutableList.of(), voidType(), emptyBlock)),
                Arguments.of(typeParameter("range"), new IExternType(new ITypeDeclaration("range", ImmutableList.of(),
                        null), false)),
                Arguments.of(new IPackageStatement("range", ImmutableList.of()),
                        new ITypeDeclaration("range", ImmutableList.of(), null)));
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
                Arguments.of(emptyBlock(), voidType(), true),
                // a block that yields is the body of no function or closure, since a yield belongs to a stream block
                Arguments.of(new IBlock(ImmutableList.of(new IYieldFragment(literal42()))), voidType(), false),
                Arguments.of(new IBlock(ImmutableList.of(new IYieldFragment(literal42()))), new IStreamType(intType()),
                        false));
    }

    @ParameterizedTest
    @MethodSource("bodiesAndResultTypes")
    void isBodyOfResultTypeMatchesValueOrIsVoid(@Nonnull final IBlock body, @Nonnull final IType resultType,
                                                final boolean expected) {
        assertThat(body.isBodyOf(resultType)).isEqualTo(expected);
    }

    @Test
    void yieldsReturnsYieldsAmongStatementsOfBlock() {
        final IYieldFragment yieldOf42 = new IYieldFragment(literal42());
        final IYieldFragment yieldOf7 = new IYieldFragment(new ILiteral(7, intType()));
        assertThat(new IBlock(ImmutableList.of(yieldOf42, new IAssignment("x", literal42()), yieldOf7)).yields())
                .containsExactly(yieldOf42, yieldOf7);
        assertThat(new IBlock(ImmutableList.of(literal42())).yields()).isEmpty();
        assertThat(emptyBlock().yields()).isEmpty();
        // the yields of a nested stream block belong to that stream block
        final IStreamFragment stream = new IStreamFragment(intType(), new IBlock(ImmutableList.of(yieldOf42)));
        assertThat(stream.body().yields()).containsExactly(yieldOf42);
        assertThat(new IBlock(ImmutableList.of(stream)).yields()).isEmpty();
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
        assertThatThrownBy(() -> new IYieldFragment(new IFunctionCall(new IVariableReference("g",
                new IFunctionType(false, ImmutableList.of(), voidType())), ImmutableList.of())))
                .isInstanceOf(IllegalArgumentException.class);
        // a yield belongs to a stream block, so neither a function, a closure, nor a program can yield, and a stream
        // block yields values of its element type, which cannot be void
        final IBlock yieldsLiteral = new IBlock(ImmutableList.of(new IYieldFragment(literal42())));
        assertThatThrownBy(() -> new IFunctionDefinition("f", ImmutableList.of(), ImmutableList.of(),
                new IStreamType(intType()), yieldsLiteral)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IClosure(ImmutableList.of(), voidType(), yieldsLiteral))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IProgram(yieldsLiteral.statements())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IStreamFragment(bigintType(), yieldsLiteral))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IStreamFragment(voidType(), emptyBlock()))
                .isInstanceOf(IllegalArgumentException.class);
        // the parameters of a declared function have distinct names
        assertThatThrownBy(() -> new IFunctionDeclaration("f", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinition.Parameter("x", intType()),
                        new IFunctionDefinition.Parameter("x", intType())), voidType()))
                .isInstanceOf(IllegalArgumentException.class);
        // a reference gives each type parameter of its function a type argument
        assertThatThrownBy(() -> new IFunctionReference(identityFunction(), ImmutableList.of(),
                new IFunctionType(false, ImmutableList.of(intType()), intType())))
                .isInstanceOf(IllegalArgumentException.class);
        // a package statement can only be the first statement of a program
        assertThatThrownBy(() -> new IProgram(ImmutableList.of(rangeDeclaration(), abcPackage())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void packageStatementOfProgramIsItsFirstStatement() {
        assertThat(new IProgram(ImmutableList.of(abcPackage(), rangeDeclaration())).packageStatement())
                .contains(abcPackage());
        assertThat(new IProgram(ImmutableList.of(rangeDeclaration())).packageStatement()).isEmpty();
        assertThat(new IProgram(ImmutableList.of()).packageStatement()).isEmpty();
    }

    @Test
    void argumentLabelFindsFirstArgumentWithLabel() {
        final IAnnotationFragment annotation = new IAnnotationFragment("java", ImmutableList.of(
                new IAnnotationFragment.Argument(null, "a"), new IAnnotationFragment.Argument("class", "b"),
                new IAnnotationFragment.Argument("class", "c")));
        assertThat(annotation.argument("class")).contains("b");
        assertThat(annotation.argument("method")).isEmpty();
    }

    @Test
    void typeOfExternTypeIsWrittenAsItsName() {
        assertThat(new IExternType(rangeDeclaration(), true)).hasToString("range?");
        assertThat(new IExternType(new ITypeDeclaration("first range", ImmutableList.of(), null), false))
                .hasToString("`first range`");
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
        return new IFunctionReference(identityFunction(), ImmutableList.of(intType()),
                new IFunctionType(false, ImmutableList.of(intType()), intType()));
    }

    /**
     * Returns {@code @java("com.apple.foundationdb.record.ir")}.
     */
    @Nonnull
    private static IAnnotationFragment javaPackageAnnotation() {
        return new IAnnotationFragment("java",
                ImmutableList.of(new IAnnotationFragment.Argument(null, "com.apple.foundationdb.record.ir")));
    }

    /**
     * Returns {@code @java("com.apple.foundationdb.record.ir") package abc}.
     */
    @Nonnull
    private static IPackageStatement abcPackage() {
        return new IPackageStatement("abc", ImmutableList.of(javaPackageAnnotation()));
    }

    /**
     * Returns {@code @java(class: "com.apple.foundationdb.record.TupleRange") extern type range} in the package
     * {@code abc}.
     */
    @Nonnull
    private static ITypeDeclaration rangeDeclaration() {
        return new ITypeDeclaration("range", ImmutableList.of(new IAnnotationFragment("java", ImmutableList.of(
                new IAnnotationFragment.Argument("class", "com.apple.foundationdb.record.TupleRange")))), abcPackage());
    }

    @Nonnull
    private static ILiteral literal42() {
        return new ILiteral(42, intType());
    }
}
