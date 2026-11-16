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
                pinned(() -> new IFunctionDefinitionFragment.Parameter("x", typeParameter("T")), -853251890),
                pinned(IFragmentTest::emptyBlock, -1386164011),
                pinned(IFragmentTest::identityFunction, -1049441388),
                pinned(() -> new IProgramFragment(ImmutableList.of(identityFunction())), -2050522768),
                pinned(() -> new IFunctionDeclarationFragment("lookup", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("key", bigintType())),
                        stringType().withNullability(true)), 688860876),
                pinned(() -> new IFunctionDeclarationFragment("scan", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("range",
                                new IExternType(rangeDeclaration(), false))),
                        intType(), ImmutableList.of(), abcPackage()), 1790714996),
                pinned(() -> new IAnnotationFragment.Argument(null, "com.apple.foundationdb.record.ir"), 1230968439),
                pinned(() -> new IAnnotationFragment.Argument("class", "com.apple.foundationdb.record.TupleRange"),
                        1319917643),
                pinned(IFragmentTest::javaPackageAnnotation, 1583677570),
                pinned(IFragmentTest::abcPackage, -1125828173),
                pinned(IFragmentTest::rangeDeclaration, 1798781256),
                pinned(() -> new IExternType(rangeDeclaration(), true), -1676417504),
                pinned(() -> new IClosureFragment.Parameter("x", intType()), 1044350755),
                pinned(() -> new IClosureFragment.Parameter("y", bigintType()), 926581826),
                pinned(() -> new IClosureFragment(ImmutableList.of(new IClosureFragment.Parameter("x", intType()),
                        new IClosureFragment.Parameter("y", bigintType())), voidType(), emptyBlock()), 1543548840),
                pinned(IFragmentTest::literal42, 1198197660),
                pinned(() -> new ILiteralFragment(42L, bigintType()), 1080428700),
                // the hash code of a half is its 16-bit pattern, 0x3E00 for 1.5
                pinned(() -> new ILiteralFragment(Half.valueOf(1.5f),
                        new IPrimitiveType(IPrimitiveType.Kind.HALF, false)), 1294493890),
                pinned(() -> new ILiteralFragment(null, stringType().withNullability(true)), -686800226),
                pinned(() -> new IVariableReferenceFragment("x", intType()), -73490170),
                pinned(IFragmentTest::identityReference, 23787472),
                pinned(() -> new IFunctionCallFragment(identityReference(), ImmutableList.of(literal42())), -761835920),
                pinned(() -> new IAssignmentFragment("x", literal42()), -405760496),
                pinned(() -> new IYieldFragment(literal42()), 467444080),
                pinned(() -> new IStreamBlockFragment(intType(),
                        new IBlockFragment(ImmutableList.of(new IYieldFragment(literal42())))), 1105828874));
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
        final IBlockFragment emptyBlock = emptyBlock();
        final IBlockFragment nonEmptyBlock = new IBlockFragment(ImmutableList.of(identityFunction()));
        final IRecordType.Field a = IRecordType.Field.of(intType, "a");
        final IRecordType.Field b = IRecordType.Field.of(bigintType, "b");
        final IVectorType vector = new IVectorType(false, IPrimitiveType.Kind.FLOAT, 3);
        final IEnumType.Value red = new IEnumType.Value("RED", 0);
        final IEnumType.Value green = new IEnumType.Value("GREEN", 1);
        final IFunctionType function = new IFunctionType(false, ImmutableList.of(intType), bigintType);
        final IFunctionDefinitionFragment id = identityFunction();
        final IFunctionDefinitionFragment.Parameter x = new IFunctionDefinitionFragment.Parameter("x", t);
        final IFunctionDeclarationFragment declaration = new IFunctionDeclarationFragment("lookup", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("key", bigintType)), intType);
        final IClosureFragment.Parameter closureX = new IClosureFragment.Parameter("x", intType);
        final IClosureFragment closure = new IClosureFragment(ImmutableList.of(closureX), intType,
                new IBlockFragment(ImmutableList.of(new IVariableReferenceFragment("x", intType))));
        final ILiteralFragment literal = literal42();
        final IFunctionType intToInt = new IFunctionType(false, ImmutableList.of(intType), intType);
        final IFunctionReferenceFragment reference = identityReference();
        final IFunctionCallFragment call = new IFunctionCallFragment(reference, ImmutableList.of(literal));
        final IAnnotationFragment.Argument packageArgument =
                new IAnnotationFragment.Argument(null, "com.apple.foundationdb.record.ir");
        final IAnnotationFragment annotation = javaPackageAnnotation();
        final IPackageStatementFragment abc = abcPackage();
        final ITypeDeclarationFragment range = rangeDeclaration();
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
                Arguments.of(new IProgramFragment(ImmutableList.of()),
                        new IProgramFragment(nonEmptyBlock.statements())),
                // function definitions: the name, the type parameters, the parameters, the result type, the body, the
                // annotations
                Arguments.of(id, new IFunctionDefinitionFragment("f", id.typeParameters(), id.parameters(),
                        id.resultType(), id.body())),
                Arguments.of(id, new IFunctionDefinitionFragment("id", ImmutableList.of(t, typeParameter("U")),
                        id.parameters(), id.resultType(), id.body())),
                Arguments.of(id, new IFunctionDefinitionFragment("id", id.typeParameters(), ImmutableList.of(),
                        id.resultType(), id.body())),
                Arguments.of(id, new IFunctionDefinitionFragment("id", id.typeParameters(), id.parameters(),
                        voidType(), id.body())),
                Arguments.of(id, new IFunctionDefinitionFragment("id", id.typeParameters(), id.parameters(),
                        id.resultType(), new IBlockFragment(ImmutableList.of(new IVariableReferenceFragment("y", t))))),
                Arguments.of(id, new IFunctionDefinitionFragment("id", id.typeParameters(), id.parameters(),
                        id.resultType(), id.body(), ImmutableList.of(annotation))),
                // parameters of function definitions: the name, the type
                Arguments.of(x, new IFunctionDefinitionFragment.Parameter("y", t)),
                Arguments.of(x, new IFunctionDefinitionFragment.Parameter("x", intType)),
                // function declarations: the name, the type parameters, the parameters, the result type, the
                // annotations, the package
                Arguments.of(declaration, new IFunctionDeclarationFragment("find", declaration.typeParameters(),
                        declaration.parameters(), declaration.resultType())),
                Arguments.of(declaration, new IFunctionDeclarationFragment("lookup", ImmutableList.of(t),
                        declaration.parameters(), declaration.resultType())),
                Arguments.of(declaration, new IFunctionDeclarationFragment("lookup", declaration.typeParameters(),
                        ImmutableList.of(), declaration.resultType())),
                Arguments.of(declaration, new IFunctionDeclarationFragment("lookup", declaration.typeParameters(),
                        declaration.parameters(), voidType())),
                Arguments.of(declaration, new IFunctionDeclarationFragment("lookup", declaration.typeParameters(),
                        declaration.parameters(), declaration.resultType(), ImmutableList.of(annotation), null)),
                Arguments.of(declaration, new IFunctionDeclarationFragment("lookup", declaration.typeParameters(),
                        declaration.parameters(), declaration.resultType(), ImmutableList.of(), abc)),
                // annotations: the name, the arguments; arguments: the label, the value
                Arguments.of(annotation, new IAnnotationFragment("swift", annotation.arguments())),
                Arguments.of(annotation, new IAnnotationFragment("java", ImmutableList.of())),
                Arguments.of(packageArgument, new IAnnotationFragment.Argument("package", packageArgument.value())),
                Arguments.of(packageArgument, new IAnnotationFragment.Argument(null, "com.apple.foundationdb")),
                // package statements: the name, the annotations
                Arguments.of(abc, new IPackageStatementFragment("def", abc.annotations())),
                Arguments.of(abc, new IPackageStatementFragment("abc", ImmutableList.of())),
                // type declarations: the name, the annotations, the package
                Arguments.of(range,
                        new ITypeDeclarationFragment("interval", range.annotations(), range.packageStatement())),
                Arguments.of(range, new ITypeDeclarationFragment("range", ImmutableList.of(),
                        range.packageStatement())),
                Arguments.of(range, new ITypeDeclarationFragment("range", range.annotations(), null)),
                // extern types: the declaration, the nullability
                Arguments.of(rangeType, new IExternType(new ITypeDeclarationFragment("interval", range.annotations(),
                        range.packageStatement()), false)),
                Arguments.of(rangeType, rangeType.withNullability(true)),
                // closures: the parameters, the result type, the body
                Arguments.of(closure, new IClosureFragment(ImmutableList.of(), closure.resultType(), closure.body())),
                Arguments.of(closure, new IClosureFragment(closure.parameters(), voidType(), closure.body())),
                Arguments.of(closure, new IClosureFragment(closure.parameters(), closure.resultType(),
                        new IBlockFragment(ImmutableList.of(literal42())))),
                // parameters of closures: the name, the type
                Arguments.of(closureX, new IClosureFragment.Parameter("y", intType)),
                Arguments.of(closureX, new IClosureFragment.Parameter("x", bigintType)),
                // literals: the value, the type
                Arguments.of(literal, new ILiteralFragment(43, intType)),
                Arguments.of(literal, new ILiteralFragment(42, intType.withNullability(true))),
                Arguments.of(new ILiteralFragment(null, intType.withNullability(true)),
                        new ILiteralFragment(null, bigintType.withNullability(true))),
                // references to variables: the name, the type
                Arguments.of(new IVariableReferenceFragment("x", intType),
                        new IVariableReferenceFragment("y", intType)),
                Arguments.of(new IVariableReferenceFragment("x", intType),
                        new IVariableReferenceFragment("x", bigintType)),
                // references to functions: the function, the type arguments, the type
                Arguments.of(reference, new IFunctionReferenceFragment(new IFunctionDefinitionFragment("f",
                        id.typeParameters(), id.parameters(), id.resultType(), id.body()), reference.typeArguments(),
                        reference.type())),
                Arguments.of(reference,
                        new IFunctionReferenceFragment(id, ImmutableList.of(bigintType), reference.type())),
                Arguments.of(reference, new IFunctionReferenceFragment(id, reference.typeArguments(),
                        new IFunctionType(false, ImmutableList.of(intType), intType.withNullability(true)))),
                // calls: the callee, the arguments
                Arguments.of(call,
                        new IFunctionCallFragment(new IVariableReferenceFragment("id", intToInt), call.arguments())),
                Arguments.of(call,
                        new IFunctionCallFragment(reference, ImmutableList.of(new ILiteralFragment(43, intType)))),
                // assignments: the name, the expression
                Arguments.of(new IAssignmentFragment("x", literal), new IAssignmentFragment("y", literal)),
                Arguments.of(new IAssignmentFragment("x", literal), new IAssignmentFragment("x", call)),
                // yields: the value
                Arguments.of(new IYieldFragment(literal), new IYieldFragment(new ILiteralFragment(43, intType))),
                // stream blocks: the element type, the block
                Arguments.of(new IStreamBlockFragment(intType, emptyBlock),
                        new IStreamBlockFragment(bigintType, emptyBlock)),
                Arguments.of(new IStreamBlockFragment(intType, emptyBlock), new IStreamBlockFragment(intType,
                        new IBlockFragment(ImmutableList.of(new IYieldFragment(literal))))),
                // fragments of different kinds whose parts are equal
                Arguments.of(emptyBlock, new IProgramFragment(ImmutableList.of())),
                Arguments.of(new IFunctionDefinitionFragment.Parameter("x", intType), closureX),
                Arguments.of(new IVariableReferenceFragment("id", intToInt), new IFunctionReferenceFragment(
                        new IFunctionDeclarationFragment("id", ImmutableList.of(),
                                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", intType)), intType),
                        ImmutableList.of(), intToInt)),
                Arguments.of(new IFunctionDeclarationFragment("g", ImmutableList.of(), ImmutableList.of(), voidType()),
                        new IFunctionDefinitionFragment("g", ImmutableList.of(), ImmutableList.of(), voidType(),
                                emptyBlock)),
                Arguments.of(typeParameter("range"),
                        new IExternType(new ITypeDeclarationFragment("range", ImmutableList.of(), null), false)),
                Arguments.of(new IPackageStatementFragment("range", ImmutableList.of()),
                        new ITypeDeclarationFragment("range", ImmutableList.of(), null)));
    }

    @ParameterizedTest
    @MethodSource("fragmentsDifferingInOnePart")
    void equalsDifferentPartIsFalse(@Nonnull final IFragment fragment, @Nonnull final IFragment other) {
        assertThat(fragment).isNotEqualTo(other);
        assertThat(other).isNotEqualTo(fragment);
    }

    @Test
    void valueLastStatementIsExpressionReturnsIt() {
        final IAssignmentFragment assignment = new IAssignmentFragment("x", literal42());
        assertThat(new IBlockFragment(ImmutableList.of(assignment, literal42())).value()).contains(literal42());
        // a block whose last statement is not an expression, or that has no statements, has no value
        assertThat(new IBlockFragment(ImmutableList.of(literal42(), assignment)).value()).isEmpty();
        assertThat(emptyBlock().value()).isEmpty();
    }

    @Nonnull
    static Stream<Arguments> bodiesAndResultTypes() {
        final IBlockFragment valueIsLiteral = new IBlockFragment(ImmutableList.of(literal42()));
        return Stream.of(
                // a value of the result type
                Arguments.of(valueIsLiteral, intType(), true),
                Arguments.of(new IBlockFragment(ImmutableList.of(new IAssignmentFragment("x", literal42()),
                        literal42())), intType(), true),
                // no value, or a value of another type: there is no promotion
                Arguments.of(emptyBlock(), intType(), false),
                Arguments.of(new IBlockFragment(ImmutableList.of(new IAssignmentFragment("x", literal42()))), intType(),
                        false),
                Arguments.of(valueIsLiteral, bigintType(), false),
                Arguments.of(valueIsLiteral, intType().withNullability(true), false),
                // a void function discards the value of its body, if there is one
                Arguments.of(valueIsLiteral, voidType(), true),
                Arguments.of(emptyBlock(), voidType(), true),
                // a block that yields is the body of no function or closure, since a yield belongs to a stream block
                Arguments.of(new IBlockFragment(ImmutableList.of(new IYieldFragment(literal42()))), voidType(), false),
                Arguments.of(new IBlockFragment(ImmutableList.of(new IYieldFragment(literal42()))),
                        new IStreamType(intType()), false));
    }

    @ParameterizedTest
    @MethodSource("bodiesAndResultTypes")
    void isBodyOfResultTypeMatchesValueOrIsVoid(@Nonnull final IBlockFragment body, @Nonnull final IType resultType,
                                                final boolean expected) {
        assertThat(body.isBodyOf(resultType)).isEqualTo(expected);
    }

    @Test
    void yieldsReturnsYieldsAmongStatementsOfBlock() {
        final IYieldFragment yieldOf42 = new IYieldFragment(literal42());
        final IYieldFragment yieldOf7 = new IYieldFragment(new ILiteralFragment(7, intType()));
        assertThat(new IBlockFragment(ImmutableList.of(yieldOf42, new IAssignmentFragment("x", literal42()), yieldOf7))
                .yields()).containsExactly(yieldOf42, yieldOf7);
        assertThat(new IBlockFragment(ImmutableList.of(literal42())).yields()).isEmpty();
        assertThat(emptyBlock().yields()).isEmpty();
        // the yields of a nested stream block belong to that stream block
        final IStreamBlockFragment stream =
                new IStreamBlockFragment(intType(), new IBlockFragment(ImmutableList.of(yieldOf42)));
        assertThat(stream.body().yields()).containsExactly(yieldOf42);
        assertThat(new IBlockFragment(ImmutableList.of(stream)).yields()).isEmpty();
    }

    @Test
    void constructorIllFormedFragmentFails() {
        // a body must end with a value of the result type, unless that is void
        assertThatThrownBy(() -> new IFunctionDefinitionFragment("f", ImmutableList.of(), ImmutableList.of(), intType(),
                emptyBlock())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IClosureFragment(ImmutableList.of(), intType(),
                new IBlockFragment(ImmutableList.of(new ILiteralFragment(42L, bigintType())))))
                .isInstanceOf(IllegalArgumentException.class);
        // void is only a result type, and it is never nullable
        assertThatThrownBy(() -> new IPrimitiveType(IPrimitiveType.Kind.VOID, true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> voidType().withNullability(true)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IArrayType(false, voidType())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IStreamType(voidType())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> IRecordType.Field.unnamedOf(voidType())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IFunctionType(false, ImmutableList.of(voidType()), intType()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IFunctionDefinitionFragment.Parameter("x", voidType()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IClosureFragment.Parameter("x", voidType()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IAssignmentFragment("x", new IFunctionCallFragment(
                new IVariableReferenceFragment("g", new IFunctionType(false, ImmutableList.of(), voidType())),
                ImmutableList.of())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IYieldFragment(new IFunctionCallFragment(new IVariableReferenceFragment("g",
                new IFunctionType(false, ImmutableList.of(), voidType())), ImmutableList.of())))
                .isInstanceOf(IllegalArgumentException.class);
        // a yield belongs to a stream block, so neither a function, a closure, nor a program can yield, and a stream
        // block yields values of its element type, which cannot be void
        final IBlockFragment yieldsLiteral = new IBlockFragment(ImmutableList.of(new IYieldFragment(literal42())));
        assertThatThrownBy(() -> new IFunctionDefinitionFragment("f", ImmutableList.of(), ImmutableList.of(),
                new IStreamType(intType()), yieldsLiteral)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IClosureFragment(ImmutableList.of(), voidType(), yieldsLiteral))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IProgramFragment(yieldsLiteral.statements()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IStreamBlockFragment(bigintType(), yieldsLiteral))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IStreamBlockFragment(voidType(), emptyBlock()))
                .isInstanceOf(IllegalArgumentException.class);
        // the parameters of a declared function have distinct names
        assertThatThrownBy(() -> new IFunctionDeclarationFragment("f", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", intType()),
                        new IFunctionDefinitionFragment.Parameter("x", intType())), voidType()))
                .isInstanceOf(IllegalArgumentException.class);
        // a reference gives each type parameter of its function a type argument
        assertThatThrownBy(() -> new IFunctionReferenceFragment(identityFunction(), ImmutableList.of(),
                new IFunctionType(false, ImmutableList.of(intType()), intType())))
                .isInstanceOf(IllegalArgumentException.class);
        // a package statement can only be the first statement of a program
        assertThatThrownBy(() -> new IProgramFragment(ImmutableList.of(rangeDeclaration(), abcPackage())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void packageStatementOfProgramIsItsFirstStatement() {
        assertThat(new IProgramFragment(ImmutableList.of(abcPackage(), rangeDeclaration())).packageStatement())
                .contains(abcPackage());
        assertThat(new IProgramFragment(ImmutableList.of(rangeDeclaration())).packageStatement()).isEmpty();
        assertThat(new IProgramFragment(ImmutableList.of()).packageStatement()).isEmpty();
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
        assertThat(new IExternType(new ITypeDeclarationFragment("first range", ImmutableList.of(), null), false))
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
    private static IBlockFragment emptyBlock() {
        return new IBlockFragment(ImmutableList.of());
    }

    /**
     * Returns {@code func id<T>(x: T): T { x }}.
     */
    @Nonnull
    private static IFunctionDefinitionFragment identityFunction() {
        return new IFunctionDefinitionFragment("id", ImmutableList.of(typeParameter("T")),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", typeParameter("T"))),
                typeParameter("T"),
                new IBlockFragment(ImmutableList.of(new IVariableReferenceFragment("x", typeParameter("T")))));
    }

    /**
     * Returns the reference to {@code id} in {@code id(42)}, with the type argument {@code int}.
     */
    @Nonnull
    private static IFunctionReferenceFragment identityReference() {
        return new IFunctionReferenceFragment(identityFunction(), ImmutableList.of(intType()),
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
    private static IPackageStatementFragment abcPackage() {
        return new IPackageStatementFragment("abc", ImmutableList.of(javaPackageAnnotation()));
    }

    /**
     * Returns {@code @java(class: "com.apple.foundationdb.record.TupleRange") extern type range} in the package
     * {@code abc}.
     */
    @Nonnull
    private static ITypeDeclarationFragment rangeDeclaration() {
        return new ITypeDeclarationFragment("range", ImmutableList.of(new IAnnotationFragment("java", ImmutableList.of(
                new IAnnotationFragment.Argument("class", "com.apple.foundationdb.record.TupleRange")))), abcPackage());
    }

    @Nonnull
    private static ILiteralFragment literal42() {
        return new ILiteralFragment(42, intType());
    }
}
