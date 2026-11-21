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
import com.apple.foundationdb.record.RecordCursor;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
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
                pinned(() -> new IFunctionDefinitionFragment.Parameter("x", typeParameter("T")), -1553751008),
                pinned(() -> new IFunctionDefinitionFragment.Parameter(null, "x", intType()), -494753699),
                pinned(IFragmentTest::emptyBlock, -1386164011),
                pinned(IFragmentTest::identityFunction, 1335093566),
                pinned(() -> new IProgramFragment(ImmutableList.of(identityFunction())), 334012186),
                pinned(() -> new IFunctionDeclarationFragment("lookup", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("key", bigintType())),
                        stringType().withNullability(true)), 81478061),
                pinned(() -> new IFunctionDeclarationFragment("scan", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("range",
                                new IExternType(rangeDeclaration(), false))),
                        intType(), ImmutableList.of(), abcPackage()), 1091272035),
                pinned(IFragmentTest::overloadedIdentity, -265424345),
                pinned(IFragmentTest::intVariant, -2068075596),
                pinned(() -> new IFunctionDeclarationFragment("id", ImmutableList.of(),
                        ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", intType())), intType(),
                        ImmutableList.of(), null, intVariant()), 32529210),
                pinned(IFragmentTest::intVariantDefinition, -1146862136),
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
                pinned(IFragmentTest::identityReference, -1950657798),
                pinned(() -> new IFunctionCallFragment(identityReference(), ImmutableList.of(literal42())),
                        -1840097146),
                pinned(() -> new IAssignmentFragment("x", literal42()), -405760496),
                pinned(() -> new IFieldAccessFragment(new IVariableReferenceFragment("r", recordOfIdAndString()), 1),
                        -2146132560),
                pinned(() -> new IYieldFragment(literal42()), 467444080),
                pinned(() -> new IStreamBlockFragment(intType(),
                        new IBlockFragment(ImmutableList.of(new IYieldFragment(literal42())))), 1105828874),
                pinned(() -> new IYieldBreakFragment(null), -2130007403),
                pinned(() -> new IYieldBreakFragment(new IVariableReferenceFragment("reason", stopReasonType())),
                        -558334027),
                pinned(IFragmentTest::stopReasonType, -1783468642),
                pinned(() -> new ITypeAliasFragment("id", bigintType()), 1852341318),
                pinned(() -> new IIfFragment(new IVariableReferenceFragment("c", booleanType()), emptyBlock(), null),
                        -1255283727),
                pinned(() -> new IForFragment("x",
                        new IVariableReferenceFragment("xs", new IArrayType(false, intType())), emptyBlock()),
                        1651197367),
                pinned(() -> new IConsumeFragment.Case("x", emptyBlock()), -1344930928),
                pinned(() -> new IConsumeFragment.Case(null, emptyBlock()), -1344934648),
                pinned(() -> new IConsumeFragment(new IVariableReferenceFragment("s", new IStreamType(intType())),
                        new IConsumeFragment.Case("x", emptyBlock()), null, stoppedCase()), 1470978896));
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
        final IVariableReferenceFragment c = new IVariableReferenceFragment("c", booleanType());
        final IIfFragment anIf = new IIfFragment(c, emptyBlock, null);
        final IVariableReferenceFragment xs = new IVariableReferenceFragment("xs", new IArrayType(false, intType));
        final IForFragment aFor = new IForFragment("x", xs, emptyBlock);
        final IVariableReferenceFragment s = new IVariableReferenceFragment("s", new IStreamType(intType));
        final IConsumeFragment.Case valueCase = new IConsumeFragment.Case("x", emptyBlock);
        final IConsumeFragment.Case stoppedCase = stoppedCase();
        final IConsumeFragment aConsume = new IConsumeFragment(s, valueCase, null, stoppedCase);
        final IVariableReferenceFragment r = new IVariableReferenceFragment("r", recordOfIdAndString());
        final IOverloadedFunctionFragment overloaded = overloadedIdentity();
        final IOverloadedFunctionFragment.Variant variant = intVariant();
        final IFunctionDefinitionFragment variantDefinition = intVariantDefinition();
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
                // parameters of function definitions: the label, the name, the type
                Arguments.of(x, new IFunctionDefinitionFragment.Parameter("to", "x", t)),
                Arguments.of(x, new IFunctionDefinitionFragment.Parameter(null, "x", t)),
                Arguments.of(x, new IFunctionDefinitionFragment.Parameter("x", "y", t)),
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
                // overloaded functions: the name, the type parameters, the parameters, the annotations
                Arguments.of(overloaded, new IOverloadedFunctionFragment("f", overloaded.typeParameters(),
                        overloaded.parameters(), overloaded.annotations())),
                Arguments.of(overloaded, new IOverloadedFunctionFragment("id", ImmutableList.of(t, typeParameter("U")),
                        overloaded.parameters(), overloaded.annotations())),
                Arguments.of(overloaded, new IOverloadedFunctionFragment("id", overloaded.typeParameters(),
                        ImmutableList.of(), overloaded.annotations())),
                Arguments.of(overloaded, new IOverloadedFunctionFragment("id", overloaded.typeParameters(),
                        overloaded.parameters(), ImmutableList.of(annotation))),
                // variant parts: the overloaded function, the type arguments
                Arguments.of(variant, new IOverloadedFunctionFragment.Variant(new IOverloadedFunctionFragment("id",
                        overloaded.typeParameters(), overloaded.parameters(), ImmutableList.of(annotation)),
                        variant.typeArguments())),
                Arguments.of(variant,
                        new IOverloadedFunctionFragment.Variant(overloaded, ImmutableList.of(bigintType))),
                // a function and the same function as a variant: the variant part
                Arguments.of(variantDefinition, new IFunctionDefinitionFragment("id", ImmutableList.of(),
                        variantDefinition.parameters(), intType, variantDefinition.body())),
                Arguments.of(new IFunctionDeclarationFragment("id", ImmutableList.of(), variantDefinition.parameters(),
                        intType), new IFunctionDeclarationFragment("id", ImmutableList.of(),
                        variantDefinition.parameters(), intType, ImmutableList.of(), null, variant)),
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
                // type aliases: the name, the type
                Arguments.of(new ITypeAliasFragment("id", bigintType), new ITypeAliasFragment("key", bigintType)),
                Arguments.of(new ITypeAliasFragment("id", bigintType), new ITypeAliasFragment("id", intType)),
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
                // field accesses: the record, the ordinal
                Arguments.of(new IFieldAccessFragment(r, 1),
                        new IFieldAccessFragment(new IVariableReferenceFragment("q", r.type()), 1)),
                Arguments.of(new IFieldAccessFragment(r, 1), new IFieldAccessFragment(r, 0)),
                // yields: the value
                Arguments.of(new IYieldFragment(literal), new IYieldFragment(new ILiteralFragment(43, intType))),
                // stream blocks: the element type, the block
                Arguments.of(new IStreamBlockFragment(intType, emptyBlock),
                        new IStreamBlockFragment(bigintType, emptyBlock)),
                Arguments.of(new IStreamBlockFragment(intType, emptyBlock), new IStreamBlockFragment(intType,
                        new IBlockFragment(ImmutableList.of(new IYieldFragment(literal))))),
                // yield breaks: the reason
                Arguments.of(new IYieldBreakFragment(null),
                        new IYieldBreakFragment(new IVariableReferenceFragment("reason", stopReasonType()))),
                // ifs: the condition, the then block, the else block
                Arguments.of(anIf, new IIfFragment(new IVariableReferenceFragment("d", booleanType()), emptyBlock,
                        null)),
                Arguments.of(anIf, new IIfFragment(c, nonEmptyBlock, null)),
                Arguments.of(anIf, new IIfFragment(c, emptyBlock, emptyBlock)),
                // for loops: the variable, the sequence, the body
                Arguments.of(aFor, new IForFragment("y", xs, emptyBlock)),
                Arguments.of(aFor, new IForFragment("x", new IVariableReferenceFragment("ys", xs.type()), emptyBlock)),
                Arguments.of(aFor, new IForFragment("x", xs, nonEmptyBlock)),
                // consumes: the stream, the value case, the end case, the stopped case; their cases: the binding, the
                // body
                Arguments.of(aConsume, new IConsumeFragment(new IVariableReferenceFragment("t", s.type()), valueCase,
                        null, stoppedCase)),
                Arguments.of(aConsume, new IConsumeFragment(s, new IConsumeFragment.Case("y", emptyBlock), null,
                        stoppedCase)),
                Arguments.of(aConsume, new IConsumeFragment(s, valueCase, new IConsumeFragment.Case(null, emptyBlock),
                        stoppedCase)),
                Arguments.of(aConsume, new IConsumeFragment(s, valueCase, null,
                        new IConsumeFragment.Case("other", stoppedCase.body()))),
                Arguments.of(valueCase, new IConsumeFragment.Case("y", emptyBlock)),
                Arguments.of(valueCase, new IConsumeFragment.Case(null, emptyBlock)),
                Arguments.of(valueCase, new IConsumeFragment.Case("x", nonEmptyBlock)),
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
                        new ITypeDeclarationFragment("range", ImmutableList.of(), null)),
                Arguments.of(new ITypeAliasFragment("x", intType), new IVariableReferenceFragment("x", intType)));
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
        final IVariableReferenceFragment condition = new IVariableReferenceFragment("c", booleanType());
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
                        new IStreamType(intType()), false),
                // also not in a block of a statement of control flow, and it cannot break a stream or consume one
                // either; a statement of control flow without them is a statement like any other
                Arguments.of(new IBlockFragment(ImmutableList.of(new IIfFragment(condition,
                        new IBlockFragment(ImmutableList.of(new IYieldFragment(literal42()))), null))), voidType(),
                        false),
                Arguments.of(new IBlockFragment(ImmutableList.of(new IYieldBreakFragment(null))), voidType(), false),
                Arguments.of(new IBlockFragment(ImmutableList.of(new IConsumeFragment(
                        new IVariableReferenceFragment("s", new IStreamType(intType())),
                        new IConsumeFragment.Case("x", emptyBlock()), null, stoppedCase()))), voidType(), false),
                Arguments.of(new IBlockFragment(ImmutableList.of(new IIfFragment(condition, valueIsLiteral, null))),
                        voidType(), true));
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
        // the blocks of statements of control flow run as part of the block, so their yields are yields of the block
        final IIfFragment ifYields = new IIfFragment(new IVariableReferenceFragment("c", booleanType()),
                new IBlockFragment(ImmutableList.of(yieldOf42)), new IBlockFragment(ImmutableList.of(yieldOf7)));
        assertThat(new IBlockFragment(ImmutableList.of(new IForFragment("x",
                new IVariableReferenceFragment("xs", new IArrayType(false, intType())),
                new IBlockFragment(ImmutableList.of(ifYields))))).yields()).containsExactly(yieldOf42, yieldOf7);
        assertThat(new IBlockFragment(ImmutableList.of(new IConsumeFragment(
                new IVariableReferenceFragment("s", new IStreamType(intType())),
                new IConsumeFragment.Case("x", new IBlockFragment(ImmutableList.of(yieldOf42))),
                new IConsumeFragment.Case(null, new IBlockFragment(ImmutableList.of(yieldOf7))), stoppedCase())))
                .yields()).containsExactly(yieldOf42, yieldOf7);
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
        // the same holds for the blocks of statements of control flow, and for yield breaks and consumes
        final IIfFragment ifYields =
                new IIfFragment(new IVariableReferenceFragment("c", booleanType()), yieldsLiteral, null);
        final IVariableReferenceFragment s = new IVariableReferenceFragment("s", new IStreamType(intType()));
        assertThatThrownBy(() -> new IProgramFragment(ImmutableList.of(ifYields)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IStreamBlockFragment(bigintType(), new IBlockFragment(ImmutableList.of(ifYields))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IFunctionDefinitionFragment("f", ImmutableList.of(), ImmutableList.of(),
                voidType(), new IBlockFragment(ImmutableList.of(new IYieldBreakFragment(null)))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IClosureFragment(ImmutableList.of(), voidType(), new IBlockFragment(
                ImmutableList.of(new IConsumeFragment(s, new IConsumeFragment.Case("x", emptyBlock()), null,
                        stoppedCase()))))).isInstanceOf(IllegalArgumentException.class);
        // the condition of an if is a boolean that is not nullable, and a for loop iterates over an array that is not
        // nullable
        assertThatThrownBy(() -> new IIfFragment(literal42(), emptyBlock(), null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IIfFragment(new IVariableReferenceFragment("c",
                booleanType().withNullability(true)), emptyBlock(), null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IForFragment("x", literal42(), emptyBlock()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IForFragment("x",
                new IVariableReferenceFragment("xs", new IArrayType(true, intType())), emptyBlock()))
                .isInstanceOf(IllegalArgumentException.class);
        // a consume consumes a stream; its value case and its stopped case bind a name, and its end case binds none
        assertThatThrownBy(() -> new IConsumeFragment(literal42(), new IConsumeFragment.Case("x", emptyBlock()), null,
                stoppedCase())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IConsumeFragment(s, new IConsumeFragment.Case(null, emptyBlock()), null,
                stoppedCase())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IConsumeFragment(s, new IConsumeFragment.Case("x", emptyBlock()),
                new IConsumeFragment.Case("y", emptyBlock()), stoppedCase()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IConsumeFragment(s, new IConsumeFragment.Case("x", emptyBlock()), null,
                new IConsumeFragment.Case(null, stoppedCase().body()))).isInstanceOf(IllegalArgumentException.class);
        // every consume has a stopped case, since the reason of a stop has to be communicated outward
        assertThatThrownBy(() -> new IConsumeFragment(s, new IConsumeFragment.Case("x", emptyBlock()), null, null))
                .isInstanceOf(NullPointerException.class);
        // the reason of a yield break is a stopReason, which is not nullable, and no other enum is one
        assertThatThrownBy(() -> new IYieldBreakFragment(literal42())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IYieldBreakFragment(new IVariableReferenceFragment("reason",
                stopReasonType().withNullability(true)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IYieldBreakFragment(new IVariableReferenceFragment("reason",
                new IEnumType(false, ImmutableList.of(new IEnumType.Value("TIME_LIMIT_REACHED", 2))))))
                .isInstanceOf(IllegalArgumentException.class);
        // the name of a type alias can be written
        assertThatThrownBy(() -> new ITypeAliasFragment("", intType())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ITypeAliasFragment("a`b", intType())).isInstanceOf(IllegalArgumentException.class);
        // the parameters of a declared function have distinct names
        assertThatThrownBy(() -> new IFunctionDeclarationFragment("f", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", intType()),
                        new IFunctionDefinitionFragment.Parameter("x", intType())), voidType()))
                .isInstanceOf(IllegalArgumentException.class);
        // the named fields of a record have distinct names, since a field access can name a field
        assertThatThrownBy(() -> new IRecordType(false, ImmutableList.of(IRecordType.Field.of(intType(), "a"),
                IRecordType.Field.of(bigintType(), "a")))).isInstanceOf(IllegalArgumentException.class);
        // a field access needs a record that is not nullable, and the ordinal of a field of that record
        final IVariableReferenceFragment r = new IVariableReferenceFragment("r", recordOfIdAndString());
        assertThatThrownBy(() -> new IFieldAccessFragment(literal42(), 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IFieldAccessFragment(new IVariableReferenceFragment("r",
                recordOfIdAndString().withNullability(true)), 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IFieldAccessFragment(r, -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IFieldAccessFragment(r, 2)).isInstanceOf(IllegalArgumentException.class);
        // a reference gives each type parameter of its function a type argument
        assertThatThrownBy(() -> new IFunctionReferenceFragment(identityFunction(), ImmutableList.of(),
                new IFunctionType(false, ImmutableList.of(intType()), intType())))
                .isInstanceOf(IllegalArgumentException.class);
        // a package statement can only be the first statement of a program
        assertThatThrownBy(() -> new IProgramFragment(ImmutableList.of(rangeDeclaration(), abcPackage())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructorIllFormedOverloadedFunctionOrVariantFails() {
        final IOverloadedFunctionFragment overloaded = overloadedIdentity();
        final IFunctionDefinitionFragment.Parameter xOfInt = new IFunctionDefinitionFragment.Parameter("x", intType());
        // an overloaded function has type parameters, which are the keys of its variants, and distinct parameters
        assertThatThrownBy(() -> new IOverloadedFunctionFragment("f", ImmutableList.of(), ImmutableList.of(),
                ImmutableList.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IOverloadedFunctionFragment("f", ImmutableList.of(typeParameter("T")),
                ImmutableList.of(xOfInt, xOfInt), ImmutableList.of())).isInstanceOf(IllegalArgumentException.class);
        // a variant has one concrete type argument for each type parameter, which is not void
        assertThatThrownBy(() -> new IOverloadedFunctionFragment.Variant(overloaded, ImmutableList.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IOverloadedFunctionFragment.Variant(overloaded,
                ImmutableList.of(intType(), intType()))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IOverloadedFunctionFragment.Variant(overloaded,
                ImmutableList.of(new IArrayType(false, typeParameter("U")))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IOverloadedFunctionFragment.Variant(overloaded, ImmutableList.of(voidType())))
                .isInstanceOf(IllegalArgumentException.class);
        // a variant has the name of its overloaded function, no type parameters, and the parameters for its key
        final IBlockFragment xOfIntIsValue =
                new IBlockFragment(ImmutableList.of(new IVariableReferenceFragment("x", intType())));
        assertThatThrownBy(() -> new IFunctionDefinitionFragment("other", ImmutableList.of(), ImmutableList.of(xOfInt),
                intType(), xOfIntIsValue, ImmutableList.of(), intVariant()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IFunctionDeclarationFragment("id", ImmutableList.of(typeParameter("U")),
                ImmutableList.of(xOfInt), intType(), ImmutableList.of(), null, intVariant()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IFunctionDeclarationFragment("id", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", bigintType())), intType(),
                ImmutableList.of(), null, intVariant())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IFunctionDeclarationFragment("id", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter(null, "x", intType())), intType(),
                ImmutableList.of(), null, intVariant())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IFunctionDeclarationFragment("id", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", "y", intType())), intType(),
                ImmutableList.of(), null, intVariant())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IFunctionDeclarationFragment("id", ImmutableList.of(), ImmutableList.of(),
                intType(), ImmutableList.of(), null, intVariant())).isInstanceOf(IllegalArgumentException.class);
        // the result type is the variant's own
        assertThat(new IFunctionDeclarationFragment("id", ImmutableList.of(), ImmutableList.of(xOfInt), stringType(),
                ImmutableList.of(), null, intVariant()).variant()).contains(intVariant());
    }

    @Test
    void parametersOfVariantReplaceTypeParametersWithTypeArguments() {
        final IGenericType a = typeParameter("A");
        final IGenericType b = typeParameter("B");
        final IOverloadedFunctionFragment overloaded = new IOverloadedFunctionFragment("f", ImmutableList.of(a, b),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter(null, "x", a.withNullability(true)),
                        new IFunctionDefinitionFragment.Parameter("with", "g",
                                new IFunctionType(false, ImmutableList.of(new IArrayType(false, b)), a)),
                        new IFunctionDefinitionFragment.Parameter("digits", intType())), ImmutableList.of());
        // a type parameter that occurs as A? becomes its type argument made nullable, also if that is nullable already;
        // a parameter without type parameters stays as it is
        assertThat(overloaded.parametersOfVariant(ImmutableList.of(bigintType(), stringType().withNullability(true))))
                .containsExactly(
                        new IFunctionDefinitionFragment.Parameter(null, "x", bigintType().withNullability(true)),
                        new IFunctionDefinitionFragment.Parameter("with", "g", new IFunctionType(false,
                                ImmutableList.of(new IArrayType(false, stringType().withNullability(true))),
                                bigintType())),
                        new IFunctionDefinitionFragment.Parameter("digits", intType()));
        assertThat(overloaded.parametersOfVariant(ImmutableList.of(intType().withNullability(true), intType())).get(0))
                .isEqualTo(new IFunctionDefinitionFragment.Parameter(null, "x", intType().withNullability(true)));
        // a stream type cannot be made nullable
        assertThatThrownBy(() ->
                overloaded.parametersOfVariant(ImmutableList.of(new IStreamType(intType()), intType())))
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
    void labelOfParameterIsItsNameUnlessStated() {
        // as in Swift, the label of `x: int` is its name; `to x: int` has the label `to`, and `_ x: int` has none
        assertThat(new IFunctionDefinitionFragment.Parameter("x", intType()).label()).contains("x");
        assertThat(new IFunctionDefinitionFragment.Parameter("to", "x", intType()).label()).contains("to");
        assertThat(new IFunctionDefinitionFragment.Parameter(null, "x", intType()).label()).isEmpty();
        assertThat(new IFunctionDefinitionFragment.Parameter("x", intType()))
                .isEqualTo(new IFunctionDefinitionFragment.Parameter("x", "x", intType()));
    }

    @Test
    void fieldOfFieldAccessIsFieldAtItsOrdinal() {
        final IVariableReferenceFragment r = new IVariableReferenceFragment("r", recordOfIdAndString());
        final IFieldAccessFragment id = new IFieldAccessFragment(r, 0);
        assertThat(id.field()).isEqualTo(IRecordType.Field.of(bigintType(), "id", 1));
        assertThat(id.type()).isEqualTo(bigintType());
        // the ordinal also accesses a field without a name
        final IFieldAccessFragment unnamed = new IFieldAccessFragment(r, 1);
        assertThat(unnamed.record()).isEqualTo(r);
        assertThat(unnamed.ordinal()).isEqualTo(1);
        assertThat(unnamed.field()).isEqualTo(IRecordType.Field.unnamedOf(stringType().withNullability(true)));
        assertThat(unnamed.type()).isEqualTo(stringType().withNullability(true));
    }

    @Test
    void constructorFieldsWithoutNamesSucceeds() {
        // only the named fields of a record must have distinct names, since a field without a name has only its ordinal
        final IRecordType record = new IRecordType(false,
                ImmutableList.of(IRecordType.Field.unnamedOf(intType()), IRecordType.Field.unnamedOf(intType())));
        assertThat(record.fields()).hasSize(2);
    }

    @Test
    void typeOfExternTypeIsWrittenAsItsName() {
        assertThat(new IExternType(rangeDeclaration(), true)).hasToString("range?");
        assertThat(new IExternType(new ITypeDeclarationFragment("first range", ImmutableList.of(), null), false))
                .hasToString("`first range`");
    }

    @Test
    void stopReasonTypeMirrorsNoNextReasonsOfLimits() {
        // a value for each reason why a cursor stops before its end, of the same name, numbered by its ordinal; the end
        // of a stream is not a stop, so SOURCE_EXHAUSTED has no value
        assertThat(IConsumeFragment.STOP_REASON_TYPE.values()).containsExactlyElementsOf(
                Arrays.stream(RecordCursor.NoNextReason.values())
                        .filter(RecordCursor.NoNextReason::isLimitReached)
                        .map(reason -> new IEnumType.Value(reason.name(), reason.ordinal()))
                        .collect(ImmutableList.toImmutableList()));
        assertThat(IConsumeFragment.STOP_REASON_TYPE).isEqualTo(stopReasonType());
        // a program names the enum by the alias stopReason
        assertThat(IConsumeFragment.STOP_REASON_ALIAS)
                .isEqualTo(new ITypeAliasFragment("stopReason", stopReasonType()));
    }

    @Test
    void blocksOfControlFlowAreTheBlocksInOrder() {
        final IBlockFragment first = new IBlockFragment(ImmutableList.of(literal42()));
        final IBlockFragment second = new IBlockFragment(ImmutableList.of(new ILiteralFragment(7, intType())));
        final IVariableReferenceFragment c = new IVariableReferenceFragment("c", booleanType());
        assertThat(new IIfFragment(c, first, null).blocks()).containsExactly(first);
        assertThat(new IIfFragment(c, first, second).blocks()).containsExactly(first, second);
        assertThat(new IForFragment("x", new IVariableReferenceFragment("xs", new IArrayType(false, intType())), first)
                .blocks()).containsExactly(first);
        final IVariableReferenceFragment s = new IVariableReferenceFragment("s", new IStreamType(intType()));
        final IConsumeFragment.Case stopped = stoppedCase();
        assertThat(new IConsumeFragment(s, new IConsumeFragment.Case("x", first), null, stopped).blocks())
                .containsExactly(first, stopped.body());
        assertThat(new IConsumeFragment(s, new IConsumeFragment.Case("x", first),
                new IConsumeFragment.Case(null, second), stopped).blocks())
                .containsExactly(first, second, stopped.body());
    }

    /**
     * Returns bodies of stopped cases that end with a yield break on every path.
     */
    @Nonnull
    static Stream<IBlockFragment> stoppedCaseBodiesEndingWithYieldBreak() {
        final IYieldBreakFragment stop =
                new IYieldBreakFragment(new IVariableReferenceFragment("reason", stopReasonType()));
        final IYieldBreakFragment end = new IYieldBreakFragment(null);
        final IVariableReferenceFragment c = new IVariableReferenceFragment("c", booleanType());
        final IBlockFragment elseIf = new IBlockFragment(ImmutableList.of(new IIfFragment(c,
                new IBlockFragment(ImmutableList.of(end)), new IBlockFragment(ImmutableList.of(stop)))));
        return Stream.of(
                // a yield break with a reason stops the stream block, and one without a reason ends its stream
                new IBlockFragment(ImmutableList.of(stop)),
                new IBlockFragment(ImmutableList.of(end)),
                // what comes before the yield break does not matter
                new IBlockFragment(ImmutableList.of(new IYieldFragment(literal42()), stop)),
                // both blocks of an if end with a yield break, also those of an else if
                new IBlockFragment(ImmutableList.of(new IIfFragment(c, new IBlockFragment(ImmutableList.of(stop)),
                        new IBlockFragment(ImmutableList.of(end))))),
                new IBlockFragment(ImmutableList.of(new IIfFragment(c, new IBlockFragment(ImmutableList.of(stop)),
                        elseIf))),
                // a consume whose end case ends with a yield break: its stopped case does too, and its value case
                // returns to its loop
                new IBlockFragment(ImmutableList.of(consumeOfT(new IConsumeFragment.Case(null,
                        new IBlockFragment(ImmutableList.of(stop)))))));
    }

    @ParameterizedTest
    @MethodSource("stoppedCaseBodiesEndingWithYieldBreak")
    void constructorStoppedCaseEndingWithYieldBreakSucceeds(@Nonnull final IBlockFragment body) {
        final IConsumeFragment.Case stoppedCase = new IConsumeFragment.Case("reason", body);
        assertThat(new IConsumeFragment(new IVariableReferenceFragment("s", new IStreamType(intType())),
                new IConsumeFragment.Case("x", emptyBlock()), null, stoppedCase).stoppedCase()).isSameAs(stoppedCase);
    }

    /**
     * Returns bodies of stopped cases with a path that does not end with a yield break.
     */
    @Nonnull
    static Stream<IBlockFragment> stoppedCaseBodiesCompletingNormally() {
        final IYieldBreakFragment stop =
                new IYieldBreakFragment(new IVariableReferenceFragment("reason", stopReasonType()));
        final IVariableReferenceFragment c = new IVariableReferenceFragment("c", booleanType());
        return Stream.of(
                emptyBlock(),
                new IBlockFragment(ImmutableList.of(new IYieldFragment(literal42()))),
                // a statement after the yield break
                new IBlockFragment(ImmutableList.of(stop, new IYieldFragment(literal42()))),
                // an if without an else, or with an else that completes normally
                new IBlockFragment(ImmutableList.of(new IIfFragment(c, new IBlockFragment(ImmutableList.of(stop)),
                        null))),
                new IBlockFragment(ImmutableList.of(new IIfFragment(c, new IBlockFragment(ImmutableList.of(stop)),
                        emptyBlock()))),
                // a for loop completes normally after its last element
                new IBlockFragment(ImmutableList.of(new IForFragment("x",
                        new IVariableReferenceFragment("xs", new IArrayType(false, intType())),
                        new IBlockFragment(ImmutableList.of(stop))))),
                // a consume leaves its loop at the end of its stream, without an end case or after one that completes
                // normally
                new IBlockFragment(ImmutableList.of(consumeOfT(null))),
                new IBlockFragment(ImmutableList.of(consumeOfT(new IConsumeFragment.Case(null, emptyBlock())))),
                // the yield breaks of a stream block end or stop the stream of that stream block
                new IBlockFragment(ImmutableList.of(new IStreamBlockFragment(intType(),
                        new IBlockFragment(ImmutableList.of(new IYieldBreakFragment(null)))))));
    }

    @ParameterizedTest
    @MethodSource("stoppedCaseBodiesCompletingNormally")
    void constructorStoppedCaseCompletingNormallyFails(@Nonnull final IBlockFragment body) {
        // the reason of a stop has to be communicated outward, so every path through the stopped case ends with a yield
        // break
        assertThatThrownBy(() -> new IConsumeFragment(new IVariableReferenceFragment("s", new IStreamType(intType())),
                new IConsumeFragment.Case("x", emptyBlock()), null, new IConsumeFragment.Case("reason", body)))
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
    private static IPrimitiveType booleanType() {
        return new IPrimitiveType(IPrimitiveType.Kind.BOOLEAN, false);
    }

    /**
     * Returns a type equal to {@code stopReason} that shares no parts with {@link IConsumeFragment#STOP_REASON_TYPE}.
     */
    @Nonnull
    private static IEnumType stopReasonType() {
        return new IEnumType(false, ImmutableList.of(new IEnumType.Value("RETURN_LIMIT_REACHED", 1),
                new IEnumType.Value("TIME_LIMIT_REACHED", 2), new IEnumType.Value("SCAN_LIMIT_REACHED", 3),
                new IEnumType.Value("BYTE_LIMIT_REACHED", 4)));
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
     * Returns {@code func id<T>(x: T) -> T { x }}.
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
     * Returns {@code overloaded func id<T>(x: T)}.
     */
    @Nonnull
    private static IOverloadedFunctionFragment overloadedIdentity() {
        return new IOverloadedFunctionFragment("id", ImmutableList.of(typeParameter("T")),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", typeParameter("T"))),
                ImmutableList.of());
    }

    /**
     * Returns the variant part of the variant {@code id<int>} of {@link #overloadedIdentity()}.
     */
    @Nonnull
    private static IOverloadedFunctionFragment.Variant intVariant() {
        return new IOverloadedFunctionFragment.Variant(overloadedIdentity(), ImmutableList.of(intType()));
    }

    /**
     * Returns {@code variant func id<int>(x: int) -> int { x }}.
     */
    @Nonnull
    private static IFunctionDefinitionFragment intVariantDefinition() {
        return new IFunctionDefinitionFragment("id", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", intType())), intType(),
                new IBlockFragment(ImmutableList.of(new IVariableReferenceFragment("x", intType()))),
                ImmutableList.of(), intVariant());
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

    /**
     * Returns {@code record<id(1): bigint, string?>}, a record with a named field at ordinal 0 and a field without a
     * name at ordinal 1.
     */
    @Nonnull
    private static IRecordType recordOfIdAndString() {
        return new IRecordType(false, ImmutableList.of(IRecordType.Field.of(bigintType(), "id", 1),
                IRecordType.Field.unnamedOf(stringType().withNullability(true))));
    }

    /**
     * Returns {@code case .stopped(reason): yield break}, a stopped case that ends the stream.
     */
    @Nonnull
    private static IConsumeFragment.Case stoppedCase() {
        return new IConsumeFragment.Case("reason",
                new IBlockFragment(ImmutableList.of(new IYieldBreakFragment(null))));
    }

    /**
     * Returns a consume of the stream {@code t} with the given end case, whose value case has no statements, and whose
     * stopped case stops for the reason of the stop of {@code t}.
     */
    @Nonnull
    private static IConsumeFragment consumeOfT(@Nullable final IConsumeFragment.Case endCase) {
        return new IConsumeFragment(new IVariableReferenceFragment("t", new IStreamType(intType())),
                new IConsumeFragment.Case("y", emptyBlock()), endCase,
                new IConsumeFragment.Case("other", new IBlockFragment(ImmutableList.of(new IYieldBreakFragment(
                        new IVariableReferenceFragment("other", stopReasonType()))))));
    }
}
