/*
 * FrlIrOperatorParsingTest.java
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

import com.apple.foundationdb.record.ir.fragments.IAnnotationFragment;
import com.apple.foundationdb.record.ir.fragments.IArrayType;
import com.apple.foundationdb.record.ir.fragments.IAssignmentFragment;
import com.apple.foundationdb.record.ir.fragments.IBlockFragment;
import com.apple.foundationdb.record.ir.fragments.IClosureFragment;
import com.apple.foundationdb.record.ir.fragments.IExpressionFragment;
import com.apple.foundationdb.record.ir.fragments.IExternType;
import com.apple.foundationdb.record.ir.fragments.IFunctionCallFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionDeclarationFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionDefinitionFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionReferenceFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionType;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.apple.foundationdb.record.ir.fragments.IIfFragment;
import com.apple.foundationdb.record.ir.fragments.ILiteralFragment;
import com.apple.foundationdb.record.ir.fragments.IOverloadedFunctionFragment;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IReturnFragment;
import com.apple.foundationdb.record.ir.fragments.IStatementFragment;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.fragments.ITypeDeclarationFragment;
import com.apple.foundationdb.record.ir.fragments.IVariableReferenceFragment;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.ir.FrlIrParsing.parseProgram;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for parsing FRL-IR operators and overloaded functions: an operator calls the function that an
 * {@code @operator} annotation in scope gives it, and a call or a reference of an overloaded function calls the one
 * variant whose key and result type fit. The tests also cover explicit type arguments, the precedence of the
 * operators, and the line breaks around them.
 */
class FrlIrOperatorParsingTest {
    private static final IType INT = new IPrimitiveType(IPrimitiveType.Kind.INT, false);
    private static final IType BIGINT = new IPrimitiveType(IPrimitiveType.Kind.BIGINT, false);
    private static final IType FLOAT = new IPrimitiveType(IPrimitiveType.Kind.FLOAT, false);
    private static final IType DOUBLE = new IPrimitiveType(IPrimitiveType.Kind.DOUBLE, false);
    private static final IType BOOLEAN = new IPrimitiveType(IPrimitiveType.Kind.BOOLEAN, false);
    private static final IType NULLABLE_INT = INT.withNullability(true);
    private static final IType NULLABLE_BIGINT = BIGINT.withNullability(true);
    private static final IGenericType A = new IGenericType("A", false);
    private static final IGenericType B = new IGenericType("B", false);
    private static final IGenericType R = new IGenericType("R", false);

    /**
     * The overloaded function of {@code +} with the three variants of the agreed table.
     */
    private static final String ADD = """
            @operator("+") overloaded func add<A, B>(a: A, b: B)
            extern variant func add<int, int>(a: int, b: int) -> int
            extern variant func add<bigint, bigint>(a: bigint, b: bigint) -> bigint
            extern variant func add<bigint, int?>(a: bigint, b: int?) -> bigint?
            let i: int = 7
            let j: int = 8
            let b: bigint = 9
            let n: int? = nil
            """;
    private static final IOverloadedFunctionFragment ADD_FUNCTION = new IOverloadedFunctionFragment("add",
            ImmutableList.of(A, B), ImmutableList.of(parameter("a", A), parameter("b", B)),
            ImmutableList.of(operatorAnnotation("+")));
    private static final IFunctionDeclarationFragment ADD_INT_INT = addVariant(INT, INT, INT);
    private static final IFunctionDeclarationFragment ADD_BIGINT_BIGINT = addVariant(BIGINT, BIGINT, BIGINT);
    private static final IFunctionDeclarationFragment ADD_BIGINT_NULLABLE_INT =
            addVariant(BIGINT, NULLABLE_INT, NULLABLE_BIGINT);

    /**
     * Operators of ints and booleans, each the operator of a plain function without labels.
     */
    private static final String OPERATORS = """
            @operator("+") extern func plus(_ x: int, _ y: int) -> int
            @operator("-") extern func minus(_ x: int, _ y: int) -> int
            @operator("*") extern func times(_ x: int, _ y: int) -> int
            @operator("-") extern func negate(_ x: int) -> int
            @operator("<") extern func less(_ x: int, _ y: int) -> boolean
            @operator("==") extern func equal(_ x: int, _ y: int) -> boolean
            @operator("!") extern func not(_ x: boolean) -> boolean
            extern func inc(_ x: int) -> int
            let a: int = 1
            let b: int = 2
            let c: int = 3
            let t: boolean = true
            """;
    private static final IFunctionDeclarationFragment PLUS = binary("plus", "+", INT);
    private static final IFunctionDeclarationFragment MINUS = binary("minus", "-", INT);
    private static final IFunctionDeclarationFragment TIMES = binary("times", "*", INT);
    private static final IFunctionDeclarationFragment LESS = binary("less", "<", BOOLEAN);
    private static final IFunctionDeclarationFragment EQUAL = binary("equal", "==", BOOLEAN);
    private static final IFunctionDeclarationFragment NEGATE = new IFunctionDeclarationFragment("negate",
            ImmutableList.of(), ImmutableList.of(unlabeled("x", INT)), INT, ImmutableList.of(operatorAnnotation("-")),
            null);
    private static final IFunctionDeclarationFragment NOT = new IFunctionDeclarationFragment("not",
            ImmutableList.of(), ImmutableList.of(unlabeled("x", BOOLEAN)), BOOLEAN,
            ImmutableList.of(operatorAnnotation("!")), null);
    private static final IFunctionDeclarationFragment INC = new IFunctionDeclarationFragment("inc",
            ImmutableList.of(), ImmutableList.of(unlabeled("x", INT)), INT);
    private static final IVariableReferenceFragment VAR_A = new IVariableReferenceFragment("a", INT);
    private static final IVariableReferenceFragment VAR_B = new IVariableReferenceFragment("b", INT);
    private static final IVariableReferenceFragment VAR_C = new IVariableReferenceFragment("c", INT);
    private static final IVariableReferenceFragment VAR_T = new IVariableReferenceFragment("t", BOOLEAN);

    /**
     * The overloaded function of {@code <} with two variants of the same type, which the other comparisons extend.
     */
    private static final String OVERLOADED_LESS = """
            @operator("<") overloaded func less<A, B>(x: A, y: B)
            extern variant func less<int, int>(x: int, y: int) -> boolean
            extern variant func less<bigint, bigint>(x: bigint, y: bigint) -> boolean
            """;
    private static final String OVERLOADED_COMPARISONS = OVERLOADED_LESS + """
            @operator(">") overloaded func greater<A, B>(x: A, y: B)
            extern variant func greater<int, int>(x: int, y: int) -> boolean
            @operator("<=") overloaded func atMost<A, B>(x: A, y: B)
            extern variant func atMost<int, int>(x: int, y: int) -> boolean
            @operator(">=") overloaded func atLeast<A, B>(x: A, y: B)
            extern variant func atLeast<int, int>(x: int, y: int) -> boolean
            """;
    private static final IOverloadedFunctionFragment LESS_FUNCTION = comparison("less", "<");
    private static final IOverloadedFunctionFragment GREATER_FUNCTION = comparison("greater", ">");
    private static final IOverloadedFunctionFragment AT_MOST_FUNCTION = comparison("atMost", "<=");
    private static final IOverloadedFunctionFragment AT_LEAST_FUNCTION = comparison("atLeast", ">=");

    @Nonnull
    static Stream<Arguments> selectedVariants() {
        final IVariableReferenceFragment i = new IVariableReferenceFragment("i", INT);
        final IVariableReferenceFragment j = new IVariableReferenceFragment("j", INT);
        final IVariableReferenceFragment b = new IVariableReferenceFragment("b", BIGINT);
        final IVariableReferenceFragment n = new IVariableReferenceFragment("n", NULLABLE_INT);
        return Stream.of(
                // the fixed types of the operands select the variant
                Arguments.of("let e = i + j", call(ADD_INT_INT, ImmutableList.of(i, j))),
                Arguments.of("let e = b + n", call(ADD_BIGINT_NULLABLE_INT, ImmutableList.of(b, n))),
                // a literal fits every variant whose parameter type it can have, so only the int operand decides here
                Arguments.of("let e = i + 1", call(ADD_INT_INT, ImmutableList.of(i, new ILiteralFragment(1, INT)))),
                // the context decides: only one variant returns int
                Arguments.of("let e: int = 1 + 2", call(ADD_INT_INT,
                        ImmutableList.of(new ILiteralFragment(1, INT), new ILiteralFragment(2, INT)))),
                // type arguments select a variant, and the literals get its parameter types
                Arguments.of("let e = 5 +<bigint, int?> 3", call(ADD_BIGINT_NULLABLE_INT,
                        ImmutableList.of(new ILiteralFragment(5L, BIGINT), new ILiteralFragment(3, NULLABLE_INT)))),
                Arguments.of("let e = add<bigint, bigint>(a: 1, b: 2)", call(ADD_BIGINT_BIGINT,
                        ImmutableList.of(new ILiteralFragment(1L, BIGINT), new ILiteralFragment(2L, BIGINT)))),
                // the call form has the labels of the overloaded function, and the dot call too
                Arguments.of("let e = add(a: i, b: j)", call(ADD_INT_INT, ImmutableList.of(i, j))),
                Arguments.of("let e = i.add(b: j)", call(ADD_INT_INT, ImmutableList.of(i, j))),
                // a reference without a call takes its variant from its context
                Arguments.of("let e: (bigint, bigint) -> bigint = add", reference(ADD_BIGINT_BIGINT)),
                // an operation is an operand like any other: the inner one selects its variant first
                Arguments.of("let e = i + j + i", call(ADD_INT_INT,
                        ImmutableList.of(call(ADD_INT_INT, ImmutableList.of(i, j)), i))));
    }

    @ParameterizedTest
    @MethodSource("selectedVariants")
    void parseProgramOperatorOrCallSelectsFittingVariant(@Nonnull final String text,
                                                          @Nonnull final IExpressionFragment expected) {
        assertThat(lastExpression(ADD + text)).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // more than one variant fits, and the default type of a literal does not decide
            "let e = 1 + 2",
            "let e = b + 1",
            "let e = add",
            // no variant fits
            "let e = i + b",
            "let e = n + i",
            "let e = \"a\" + \"b\"",
            "let e: string = i + j",
            "let e = i +<string, string> j",
            // type arguments are one for each type parameter
            "let e = i +<int> j",
            "let e = add<int, int, int>(a: i, b: j)",
            // the call form has the labels of the overloaded function, the operator form has none
            "let e = add(i, j)",
            "let e = add(b: j, a: i)",
            // no function is the function of these operators
            "let e = i - j",
            "let e = -i",
            "let e = i * j",
    })
    void parseProgramAmbiguousOrUnfitVariantFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(ADD + text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseProgramCallSeesVariantsDeclaredBeforeIt() {
        // a call sees only the variants that are declared before it, so the call fails before the first variant
        assertThatThrownBy(() -> parseProgram("""
                @operator("+") overloaded func add<A, B>(a: A, b: B)
                let e = 1 + 2
                extern variant func add<int, int>(a: int, b: int) -> int"""))
                .isInstanceOf(IllegalArgumentException.class);
        // with one variant, a literal has only one variant to fit
        final List<IStatementFragment> statements = parseProgram("""
                @operator("+") overloaded func add<A, B>(a: A, b: B)
                extern variant func add<int, int>(a: int, b: int) -> int
                let e = 1 + 2
                extern variant func add<bigint, bigint>(a: bigint, b: bigint) -> bigint""").statements();
        assertThat(statements.get(2)).isEqualTo(new IAssignmentFragment("e", call(ADD_INT_INT,
                ImmutableList.of(new ILiteralFragment(1, INT), new ILiteralFragment(2, INT)))));
    }

    @Test
    void parseProgramResultTypeOfContextSelectsVariant() {
        final String convert = """
                overloaded func convert<A, R>(x: A)
                extern variant func convert<int, double>(x: int) -> double
                extern variant func convert<int, float>(x: int) -> float
                let i: int = 1
                """;
        final IOverloadedFunctionFragment convertFunction = new IOverloadedFunctionFragment("convert",
                ImmutableList.of(A, R), ImmutableList.of(parameter("x", A)), ImmutableList.of());
        final IFunctionDeclarationFragment toDouble = new IFunctionDeclarationFragment("convert", ImmutableList.of(),
                ImmutableList.of(parameter("x", INT)), DOUBLE, ImmutableList.of(), null,
                new IOverloadedFunctionFragment.Variant(convertFunction, ImmutableList.of(INT, DOUBLE)));
        final IFunctionDeclarationFragment toFloat = new IFunctionDeclarationFragment("convert", ImmutableList.of(),
                ImmutableList.of(parameter("x", INT)), FLOAT, ImmutableList.of(), null,
                new IOverloadedFunctionFragment.Variant(convertFunction, ImmutableList.of(INT, FLOAT)));
        final IVariableReferenceFragment i = new IVariableReferenceFragment("i", INT);
        // the two variants take the same argument; the key holds the result type, so the context decides
        assertThat(lastExpression(convert + "let e: double = convert(x: i)"))
                .isEqualTo(call(toDouble, ImmutableList.of(i)));
        assertThat(lastExpression(convert + "let e = convert<int, float>(x: i)"))
                .isEqualTo(call(toFloat, ImmutableList.of(i)));
        assertThatThrownBy(() -> parseProgram(convert + "let e = convert(x: i)"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseProgramVariantDefinitionIsCalledByItsKey() {
        final List<IStatementFragment> statements = parseProgram("""
                @operator("+") overloaded func add<A, B>(a: A, b: B)
                variant func add<int, int>(a: int, b: int) -> int { a }
                @operator("<") func less(_ x: int, _ y: int) -> boolean { true }
                let e = 1 +<int, int> 2 < 3""").statements();
        final IFunctionDefinitionFragment addIntInt = new IFunctionDefinitionFragment("add", ImmutableList.of(),
                ImmutableList.of(parameter("a", INT), parameter("b", INT)), INT,
                new IBlockFragment(ImmutableList.of(new IVariableReferenceFragment("a", INT))), ImmutableList.of(),
                new IOverloadedFunctionFragment.Variant(ADD_FUNCTION, ImmutableList.of(INT, INT)));
        assertThat(statements.get(0)).isEqualTo(ADD_FUNCTION);
        assertThat(statements.get(1)).isEqualTo(addIntInt);
        // + binds more strongly than <, and an @operator can be on a function with a body
        final IFunctionDefinitionFragment less = (IFunctionDefinitionFragment)statements.get(2);
        assertThat(statements.get(3)).isEqualTo(new IAssignmentFragment("e", call(less, ImmutableList.of(
                call(addIntInt, ImmutableList.of(new ILiteralFragment(1, INT), new ILiteralFragment(2, INT))),
                new ILiteralFragment(3, INT)))));
    }

    @Nonnull
    static Stream<Arguments> operations() {
        return Stream.of(
                // the operators of a level apply from left to right
                Arguments.of("a - b - c", call(MINUS, ImmutableList.of(call(MINUS, ImmutableList.of(VAR_A, VAR_B)),
                        VAR_C))),
                Arguments.of("a - b + c", call(PLUS, ImmutableList.of(call(MINUS, ImmutableList.of(VAR_A, VAR_B)),
                        VAR_C))),
                // * binds more strongly than + and -, which bind more strongly than the comparisons
                Arguments.of("a + b * c", call(PLUS, ImmutableList.of(VAR_A, call(TIMES,
                        ImmutableList.of(VAR_B, VAR_C))))),
                Arguments.of("a * b + c", call(PLUS, ImmutableList.of(call(TIMES, ImmutableList.of(VAR_A, VAR_B)),
                        VAR_C))),
                Arguments.of("a + b < c * a", call(LESS, ImmutableList.of(call(PLUS, ImmutableList.of(VAR_A, VAR_B)),
                        call(TIMES, ImmutableList.of(VAR_C, VAR_A))))),
                Arguments.of("a == b", call(EQUAL, ImmutableList.of(VAR_A, VAR_B))),
                // the prefix operators bind most strongly, a postfix more strongly still
                Arguments.of("-a * b", call(TIMES, ImmutableList.of(call(NEGATE, ImmutableList.of(VAR_A)), VAR_B))),
                Arguments.of("-a.inc()", call(NEGATE, ImmutableList.of(call(INC, ImmutableList.of(VAR_A))))),
                Arguments.of("!t", call(NOT, ImmutableList.of(VAR_T))),
                Arguments.of("!!t", call(NOT, ImmutableList.of(call(NOT, ImmutableList.of(VAR_T))))),
                Arguments.of("-a < b", call(LESS, ImmutableList.of(call(NEGATE, ImmutableList.of(VAR_A)), VAR_B))),
                // the sign of a literal belongs to the literal
                Arguments.of("-1", new ILiteralFragment(-1, INT)),
                Arguments.of("-1 * b", call(TIMES, ImmutableList.of(new ILiteralFragment(-1, INT), VAR_B))),
                Arguments.of("a - -1", call(MINUS, ImmutableList.of(VAR_A, new ILiteralFragment(-1, INT)))),
                Arguments.of("a-1", call(MINUS, ImmutableList.of(VAR_A, new ILiteralFragment(1, INT)))),
                Arguments.of("- a", call(NEGATE, ImmutableList.of(VAR_A))),
                // the operator form ignores labels
                Arguments.of("inc(a) + b", call(PLUS, ImmutableList.of(call(INC, ImmutableList.of(VAR_A)), VAR_B))),
                // a line break can follow an operator
                Arguments.of("a +\n    b", call(PLUS, ImmutableList.of(VAR_A, VAR_B))),
                // an operator with a space after it can start a line, also after a blank line and a comment
                Arguments.of("a\n    - b", call(MINUS, ImmutableList.of(VAR_A, VAR_B))),
                Arguments.of("a\n\n    // and then\n    + b\n    * c", call(PLUS, ImmutableList.of(VAR_A,
                        call(TIMES, ImmutableList.of(VAR_B, VAR_C))))),
                Arguments.of("a\n    /* less */ < b", call(LESS, ImmutableList.of(VAR_A, VAR_B))),
                Arguments.of("a\n\t== b", call(EQUAL, ImmutableList.of(VAR_A, VAR_B))),
                // parentheses group, so they override the precedence and the order of the operators
                Arguments.of("(a + b) * c", call(TIMES, ImmutableList.of(call(PLUS, ImmutableList.of(VAR_A, VAR_B)),
                        VAR_C))),
                Arguments.of("a - (b - c)", call(MINUS, ImmutableList.of(VAR_A, call(MINUS,
                        ImmutableList.of(VAR_B, VAR_C))))),
                Arguments.of("-(a + b)", call(NEGATE, ImmutableList.of(call(PLUS, ImmutableList.of(VAR_A, VAR_B))))),
                Arguments.of("!(a < b)", call(NOT, ImmutableList.of(call(LESS, ImmutableList.of(VAR_A, VAR_B))))),
                // a postfix applies to the value in the parentheses
                Arguments.of("(a + b).inc()", call(INC, ImmutableList.of(call(PLUS, ImmutableList.of(VAR_A, VAR_B))))),
                // the sign of a literal belongs to the literal only without parentheses
                Arguments.of("-(1)", call(NEGATE, ImmutableList.of(new ILiteralFragment(1, INT)))),
                // in the parentheses, the line breaks follow the rules of other expressions
                Arguments.of("(a\n    - b)", call(MINUS, ImmutableList.of(VAR_A, VAR_B))),
                Arguments.of("(\n    a + b\n) * c", call(TIMES, ImmutableList.of(call(PLUS,
                        ImmutableList.of(VAR_A, VAR_B)), VAR_C))));
    }

    @ParameterizedTest
    @MethodSource("operations")
    void parseProgramOperationIsCallOfFunctionOfOperator(@Nonnull final String expression,
                                                         @Nonnull final IExpressionFragment expected) {
        assertThat(lastExpression(OPERATORS + "let e = " + expression)).isEqualTo(expected);
    }

    @Test
    void parseProgramOperatorAttachedToOperandStartsStatement() {
        // -b starts a statement of its own, a negation, because the minus is attached to b
        final List<IStatementFragment> statements = parseProgram(OPERATORS + "let e = a\n-b").statements();
        final int count = statements.size();
        assertThat(statements.get(count - 2)).isEqualTo(new IAssignmentFragment("e", VAR_A));
        assertThat(statements.get(count - 1)).isEqualTo(call(NEGATE, ImmutableList.of(VAR_B)));
        // !t too, since ! is only a prefix operator
        final List<IStatementFragment> withNot = parseProgram(OPERATORS + "let e = t\n    !t").statements();
        assertThat(withNot.get(withNot.size() - 1)).isEqualTo(call(NOT, ImmutableList.of(VAR_T)));
        // after an operator at the end of a line, !t is the operand
        assertThat(lastExpression(OPERATORS + "@operator(\"!=\") extern func differ(_ x: boolean, _ y: boolean) -> "
                + "boolean\nlet e = t !=\n    !t")).isEqualTo(call(new IFunctionDeclarationFragment("differ",
                ImmutableList.of(), ImmutableList.of(unlabeled("x", BOOLEAN), unlabeled("y", BOOLEAN)), BOOLEAN,
                ImmutableList.of(operatorAnnotation("!=")), null), ImmutableList.of(VAR_T,
                call(NOT, ImmutableList.of(VAR_T)))));
    }

    @Test
    void parseProgramGreaterThanAtStartOfLineKeepsStatementsApart() {
        // a > at the start of a line that a line break follows does not continue the line before: the line break
        // after it ends the declaration
        final List<IStatementFragment> statements =
                parseProgram("extern func r() -> record<\n    a: int\n>\nlet e = 1").statements();
        assertThat(statements).hasSize(2);
    }

    @Test
    void parseProgramOperatorInConditionIsCall() {
        final List<IStatementFragment> statements = parseProgram(OPERATORS
                + "func f() {\n    if !t { }\n    if a < b { }\n    if (!t) { }\n}").statements();
        final List<IStatementFragment> body =
                ((IFunctionDefinitionFragment)statements.get(statements.size() - 1)).body().statements();
        final IBlockFragment empty = new IBlockFragment(ImmutableList.of());
        // a condition in parentheses, as in the sketch, is the same condition
        assertThat(body).containsExactly(new IIfFragment(call(NOT, ImmutableList.of(VAR_T)), empty, null),
                new IIfFragment(call(LESS, ImmutableList.of(VAR_A, VAR_B)), empty, null),
                new IIfFragment(call(NOT, ImmutableList.of(VAR_T)), empty, null));
    }

    @Test
    void parseProgramTypeArgumentsBeforeParenthesisAreTypeArguments() {
        // as in C# and Swift, `<` after a name starts type arguments where the text after it can be type arguments:
        // here `a` and `b` are extern types and variables at the same time, since types and values have separate
        // namespaces
        final String text = """
                @operator("<") extern func less(_ x: int, _ y: int) -> boolean
                @operator(">") extern func greater(_ x: int, _ y: int) -> boolean
                extern type a
                extern type b
                extern func f<T, U>(_ x: int) -> int
                extern func g(_ x: int) -> int
                let a: int = 1
                let b: int = 2
                let e = g(f < a, b > (3))""";
        final IType typeA = new IExternType(new ITypeDeclarationFragment("a", ImmutableList.of(), null), false);
        final IType typeB = new IExternType(new ITypeDeclarationFragment("b", ImmutableList.of(), null), false);
        final IGenericType t = new IGenericType("T", false);
        final IGenericType u = new IGenericType("U", false);
        final IFunctionDeclarationFragment f = new IFunctionDeclarationFragment("f", ImmutableList.of(t, u),
                ImmutableList.of(unlabeled("x", INT)), INT);
        final IFunctionDeclarationFragment g = new IFunctionDeclarationFragment("g", ImmutableList.of(),
                ImmutableList.of(unlabeled("x", INT)), INT);
        final IFunctionType intToInt = new IFunctionType(false, ImmutableList.of(INT), INT);
        assertThat(lastExpression(text)).isEqualTo(call(g, ImmutableList.of(new IFunctionCallFragment(
                new IFunctionReferenceFragment(f, ImmutableList.of(typeA, typeB), intToInt),
                ImmutableList.of(new ILiteralFragment(3, INT))))));
    }

    @Test
    void parseProgramTypeArgumentsFixTypeParametersOfGenericFunction() {
        final IGenericType t = new IGenericType("T", false);
        final IFunctionDeclarationFragment id = new IFunctionDeclarationFragment("id", ImmutableList.of(t),
                ImmutableList.of(parameter("x", t)), t);
        final IFunctionType bigintToBigint = new IFunctionType(false, ImmutableList.of(BIGINT), BIGINT);
        assertThat(lastExpression("extern func id<T>(x: T) -> T\nlet e = id<bigint>(x: 42)")).isEqualTo(
                new IFunctionCallFragment(new IFunctionReferenceFragment(id, ImmutableList.of(BIGINT), bigintToBigint),
                        ImmutableList.of(new ILiteralFragment(42L, BIGINT))));
        // a reference with type arguments needs no context
        assertThat(lastExpression("extern func id<T>(x: T) -> T\nlet e = id<bigint>"))
                .isEqualTo(new IFunctionReferenceFragment(id, ImmutableList.of(BIGINT), bigintToBigint));
        // and a dot call can take them too
        assertThat(lastExpression("extern func id<T>(x: T) -> T\nlet e = 42.id<bigint>()")).isEqualTo(
                new IFunctionCallFragment(new IFunctionReferenceFragment(id, ImmutableList.of(BIGINT), bigintToBigint),
                        ImmutableList.of(new ILiteralFragment(42L, BIGINT))));
    }

    @Test
    void parseProgramInnerOperatorHidesOuterOne() {
        final List<IStatementFragment> statements = parseProgram("""
                @operator("+") extern func plus(_ x: int, _ y: int) -> int
                func f(a: int) -> bigint {
                    @operator("+") extern func join(_ x: int, _ y: int) -> bigint
                    return a + a
                }""").statements();
        final List<IStatementFragment> body = ((IFunctionDefinitionFragment)statements.get(1)).body().statements();
        final IFunctionDeclarationFragment join = (IFunctionDeclarationFragment)body.get(0);
        final IVariableReferenceFragment a = new IVariableReferenceFragment("a", INT);
        assertThat(body.get(1)).isEqualTo(new IReturnFragment(call(join, ImmutableList.of(a, a))));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // comparisons do not chain
            "let e = a < b < c",
            "let e = a < b == t",
            // there is no prefix +, and an operator attached to its operand does not continue the line before
            "let e = +a",
            "let e = a\n    +b",
            // a line break before an operator ends the statement unless whitespace follows the operator
            "let e = a\n+b",
            // in parentheses too, an operator attached to its operand starts no continuation
            "let e = (a\n-b)",
            // the operand of a prefix operator stays on its line
            "let e = -\na",
            // the operands must have the types of the parameters
            "let e = a + t",
            "let e = !a",
            "let e: int = a < b",
            // a plain function takes no type arguments, and a variable takes none either
            "let e = a +<int, int> b",
            "let e = inc<int>(a)",
            "let e = a<int>",
            // an operator with type arguments continues an expression only at the end of a line
            "@operator(\"*\") overloaded func mul<A>(x: A, y: A)\n"
                    + "extern variant func mul<int>(x: int, y: int) -> int\nlet e = a\n    *<int> b",
            // `>=` is one token, so a generic type right before `=` needs a space
            "extern func xs() -> [int]\nlet e: array<int>= xs()",
    })
    void parseProgramInvalidOperationFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(OPERATORS + text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseProgramGenericTypeBeforeEqualsWithSpaceParses() {
        assertThat(lastExpression("extern func xs() -> [int]\nlet e: array<int> = xs()").type())
                .isEqualTo(new IArrayType(false, INT));
    }

    @Test
    void parseProgramOperatorWithTypeArgumentsAtEndOfLineContinues() {
        assertThat(lastExpression(ADD + "let e = i +<int, int>\n    j")).isEqualTo(call(ADD_INT_INT,
                ImmutableList.of(new IVariableReferenceFragment("i", INT), new IVariableReferenceFragment("j", INT))));
    }

    @Nonnull
    static Stream<Arguments> comparisonsWithTypeArguments() {
        final ILiteralFragment one = new ILiteralFragment(1, INT);
        final ILiteralFragment two = new ILiteralFragment(2, INT);
        final IFunctionDeclarationFragment lessOfInts = comparisonVariant(LESS_FUNCTION, INT);
        return Stream.of(
                // the type arguments follow the operator: there is no << token, so `<<` is the operator and the `<`
                // of its type arguments, and a space between them is allowed
                Arguments.of("1 <<int, int> 2", call(lessOfInts, ImmutableList.of(one, two))),
                Arguments.of("1 < <int, int> 2", call(lessOfInts, ImmutableList.of(one, two))),
                Arguments.of("1 <<bigint, bigint> 2", call(comparisonVariant(LESS_FUNCTION, BIGINT),
                        ImmutableList.of(new ILiteralFragment(1L, BIGINT), new ILiteralFragment(2L, BIGINT)))),
                Arguments.of("1 ><int, int> 2", call(comparisonVariant(GREATER_FUNCTION, INT),
                        ImmutableList.of(one, two))),
                Arguments.of("1 <=<int, int> 2", call(comparisonVariant(AT_MOST_FUNCTION, INT),
                        ImmutableList.of(one, two))),
                Arguments.of("1 >=<int, int> 2", call(comparisonVariant(AT_LEAST_FUNCTION, INT),
                        ImmutableList.of(one, two))),
                // the call form selects the same variant
                Arguments.of("less<int, int>(x: 1, y: 2)", call(lessOfInts, ImmutableList.of(one, two))),
                // at the end of a line, the operator with its type arguments continues the expression
                Arguments.of("1 <<int, int>\n    2", call(lessOfInts, ImmutableList.of(one, two))));
    }

    @ParameterizedTest
    @MethodSource("comparisonsWithTypeArguments")
    void parseProgramComparisonWithTypeArgumentsSelectsVariant(@Nonnull final String expression,
                                                               @Nonnull final IExpressionFragment expected) {
        assertThat(lastExpression(OVERLOADED_COMPARISONS + "let e = " + expression)).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // both variants of less fit two literals
            "let e = 1 < 2",
            // an operator with type arguments at the start of a line starts a statement, which cannot start with <
            "let e = 1\n    <<int, int> 2",
    })
    void parseProgramComparisonWithoutFittingTypeArgumentsFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(OVERLOADED_COMPARISONS + text))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseProgramLiteralAfterIntSelectsVariantOfInt() {
        final IFunctionDeclarationFragment lessOfInts = comparisonVariant(LESS_FUNCTION, INT);
        final IVariableReferenceFragment x = new IVariableReferenceFragment("x", INT);
        final ILiteralFragment one = new ILiteralFragment(1, INT);
        final IFunctionCallFragment xLessThanOne = call(lessOfInts, ImmutableList.of(x, one));
        // a statement of a program has its own inference, so x is an int, which only the variant <int, int> takes
        assertThat(lastExpression(OVERLOADED_LESS + "let x = 5\nlet e = x < 1")).isEqualTo(xLessThanOne);
        // with a variant <int, int?> besides, type arguments or an operand of a fixed type select the variant
        final String withNullableVariant =
                OVERLOADED_LESS + "extern variant func less<int, int?>(x: int, y: int?) -> boolean\nlet x = 5\n";
        assertThat(lastExpression(withNullableVariant + "let e = x <<int, int> 1")).isEqualTo(xLessThanOne);
        assertThat(lastExpression(withNullableVariant + "let one: int = 1\nlet e = x < one"))
                .isEqualTo(call(lessOfInts, ImmutableList.of(x, new IVariableReferenceFragment("one", INT))));
        // a statement of a function body has its own inference too
        final List<IStatementFragment> statements = parseProgram(OVERLOADED_LESS
                + "func f() -> boolean {\n    let x = 5\n    return x < 1\n}").statements();
        assertThat(((IFunctionDefinitionFragment)statements.get(statements.size() - 1)).body().statements().get(1))
                .isEqualTo(new IReturnFragment(xLessThanOne));
        // in a closure, the statements share one inference, so the type of x must be stated
        final IExpressionFragment closure = lastExpression(OVERLOADED_LESS
                + "let e = { () -> boolean in\n    let x: int = 5\n    return x < 1\n}");
        assertThat(((IClosureFragment)closure).body().statements()).containsExactly(
                new IAssignmentFragment("x", new ILiteralFragment(5, INT)), new IReturnFragment(xLessThanOne));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // a variant that takes an int first and another type second makes the literal fit two variants
            "extern variant func less<int, bigint>(x: int, y: bigint) -> boolean\nlet x = 5\nlet e = x < 1",
            "extern variant func less<int, int?>(x: int, y: int?) -> boolean\nlet x = 5\nlet e = x < 1",
            // in a closure, 5 and 1 are both still literals when the variant is chosen
            "let e = { () -> boolean in\n    let x = 5\n    x < 1\n}",
    })
    void parseProgramLiteralAfterIntFitsSeveralVariantsFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(OVERLOADED_LESS + text)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // a variant needs its overloaded function before it, in the same scope
            "extern variant func add<int, int>(a: int, b: int) -> int",
            "extern variant func add<int, int>(a: int, b: int) -> int\noverloaded func add<A, B>(a: A, b: B)",
            "overloaded func add<A, B>(a: A, b: B)\n"
                    + "func f() {\n    extern variant func add<int, int>(a: int, b: int) -> int\n}",
            "extern func add(a: int, b: int) -> int\nextern variant func add<int, int>(a: int, b: int) -> int",
            // an overloaded function has type parameters, and its name is taken in its scope
            "overloaded func add(a: int, b: int)",
            "overloaded func add<A, B>(a: A, b: B)\nlet add = 1",
            "overloaded func add<A, B>(a: A, b: B)\nextern func add(a: int, b: int) -> int",
            "overloaded func add<A, B>(a: A, b: B)\noverloaded func add<A, B>(a: A, b: B)",
            // a variant has one concrete type argument for each type parameter
            "overloaded func add<A, B>(a: A, b: B)\nextern variant func add<int>(a: int, b: int) -> int",
            "overloaded func add<A, B>(a: A, b: B)\nextern variant func add<int, void>(a: int, b: int) -> int",
            "func f<T>(x: T) {\n    overloaded func add<A, B>(a: A, b: B)\n"
                    + "    extern variant func add<T, T>(a: T, b: T) -> T\n}",
            // a variant has the parameters of its overloaded function for its key, with the same names and labels
            "overloaded func add<A, B>(a: A, b: B)\nextern variant func add<int, int>(a: int, b: bigint) -> int",
            "overloaded func add<A, B>(a: A, b: B)\nextern variant func add<int, int>(_ a: int, b: int) -> int",
            "overloaded func add<A, B>(a: A, b: B)\nextern variant func add<int, int>(a: int, c: int) -> int",
            "overloaded func add<A, B>(a: A, b: B)\nextern variant func add<int, int>(a: int) -> int",
            "overloaded func f<A>(x: A, digits: int)\nextern variant func f<int>(x: int, digits: bigint) -> int",
            // no two variants have the same key
            "overloaded func add<A, B>(a: A, b: B)\nextern variant func add<int, int>(a: int, b: int) -> int\n"
                    + "extern variant func add<int, int>(a: int, b: int) -> bigint",
            // the body of a variant must have its result type
            "overloaded func add<A, B>(a: A, b: B)\nvariant func add<int, int>(a: int, b: int) -> bigint { a }",
            // an @operator annotation is on a function that is not a variant, with one operator of its parameters
            "@operator(\"+\") extern type range",
            "@operator(\"+\") package p",
            "overloaded func add<A, B>(a: A, b: B)\n"
                    + "@operator(\"+\") extern variant func add<int, int>(a: int, b: int) -> int",
            "@operator(\"+\") extern func f(x: int) -> int",
            "@operator(\"!\") extern func f(x: int, y: int) -> int",
            "@operator(\"&&\") extern func f(x: boolean, y: boolean) -> boolean",
            "@operator(\"**\") extern func f(x: int, y: int) -> int",
            "@operator(\"+\", \"-\") extern func f(x: int, y: int) -> int",
            "@operator(symbol: \"+\") extern func f(x: int, y: int) -> int",
            "@operator extern func f(x: int, y: int) -> int",
            // a scope declares an operator once at most
            "@operator(\"+\") extern func f(x: int, y: int) -> int\n"
                    + "@operator(\"+\") extern func g(x: int, y: int) -> int",
    })
    void parseProgramInvalidOverloadedFunctionOrOperatorFails(@Nonnull final String text) {
        assertThatThrownBy(() -> parseProgram(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseProgramVariantKeepsParameterWithoutTypeParameter() {
        // a parameter whose type has no type parameter is the same in every variant
        final List<IStatementFragment> statements = parseProgram("""
                overloaded func round<A>(value: A, digits: int)
                extern variant func round<double>(value: double, digits: int) -> double
                extern variant func round<float>(value: float, digits: int) -> float
                let e: float = round(value: 1.5, digits: 2)""").statements();
        final IOverloadedFunctionFragment round = (IOverloadedFunctionFragment)statements.get(0);
        assertThat(round.parameters()).containsExactly(parameter("value", A), parameter("digits", INT));
        final IFunctionFragment toFloat = (IFunctionFragment)statements.get(2);
        assertThat(toFloat.variant().map(IOverloadedFunctionFragment.Variant::typeArguments))
                .contains(ImmutableList.of(FLOAT));
        assertThat(statements.get(3)).isEqualTo(new IAssignmentFragment("e", call(toFloat,
                ImmutableList.of(new ILiteralFragment(1.5f, FLOAT), new ILiteralFragment(2, INT)))));
    }

    /**
     * Returns the expression of the last statement of a program, which is an assignment or an expression.
     */
    @Nonnull
    private static IExpressionFragment lastExpression(@Nonnull final String text) {
        final List<IStatementFragment> statements = parseProgram(text).statements();
        final IStatementFragment last = statements.get(statements.size() - 1);
        return last instanceof IAssignmentFragment assignment ? assignment.expression() : (IExpressionFragment)last;
    }

    /**
     * Returns a call of a function that has no type parameters, such as a variant.
     */
    @Nonnull
    private static IFunctionCallFragment call(@Nonnull final IFunctionFragment function,
                                              @Nonnull final List<IExpressionFragment> arguments) {
        return new IFunctionCallFragment(reference(function), arguments);
    }

    @Nonnull
    private static IFunctionReferenceFragment reference(@Nonnull final IFunctionFragment function) {
        return new IFunctionReferenceFragment(function, ImmutableList.of(), function.type());
    }

    /**
     * Returns the variant of {@code add} for the given key, as {@code extern variant func} declares it.
     */
    @Nonnull
    private static IFunctionDeclarationFragment addVariant(@Nonnull final IType a, @Nonnull final IType b,
                                                           @Nonnull final IType resultType) {
        return new IFunctionDeclarationFragment("add", ImmutableList.of(),
                ImmutableList.of(parameter("a", a), parameter("b", b)), resultType, ImmutableList.of(), null,
                new IOverloadedFunctionFragment.Variant(ADD_FUNCTION, ImmutableList.of(a, b)));
    }

    /**
     * Returns the declaration of a binary operator of ints, {@code @operator(symbol) extern func name(_ x: int,
     * _ y: int) -> resultType}.
     */
    @Nonnull
    private static IFunctionDeclarationFragment binary(@Nonnull final String name, @Nonnull final String symbol,
                                                       @Nonnull final IType resultType) {
        return new IFunctionDeclarationFragment(name, ImmutableList.of(),
                ImmutableList.of(unlabeled("x", INT), unlabeled("y", INT)), resultType,
                ImmutableList.of(operatorAnnotation(symbol)), null);
    }

    /**
     * Returns the overloaded function of a comparison,
     * {@code @operator(symbol) overloaded func name<A, B>(x: A, y: B)}.
     */
    @Nonnull
    private static IOverloadedFunctionFragment comparison(@Nonnull final String name, @Nonnull final String symbol) {
        return new IOverloadedFunctionFragment(name, ImmutableList.of(A, B),
                ImmutableList.of(parameter("x", A), parameter("y", B)), ImmutableList.of(operatorAnnotation(symbol)));
    }

    /**
     * Returns the variant of a comparison whose two operands have the given type, as
     * {@code extern variant func name<type, type>(x: type, y: type) -> boolean} declares it.
     */
    @Nonnull
    private static IFunctionDeclarationFragment comparisonVariant(@Nonnull final IOverloadedFunctionFragment comparison,
                                                                  @Nonnull final IType type) {
        return new IFunctionDeclarationFragment(comparison.name(), ImmutableList.of(),
                ImmutableList.of(parameter("x", type), parameter("y", type)), BOOLEAN, ImmutableList.of(), null,
                new IOverloadedFunctionFragment.Variant(comparison, ImmutableList.of(type, type)));
    }

    @Nonnull
    private static IAnnotationFragment operatorAnnotation(@Nonnull final String symbol) {
        return new IAnnotationFragment("operator", ImmutableList.of(new IAnnotationFragment.Argument(null, symbol)));
    }

    @Nonnull
    private static IFunctionDefinitionFragment.Parameter parameter(@Nonnull final String name,
                                                                   @Nonnull final IType type) {
        return new IFunctionDefinitionFragment.Parameter(name, type);
    }

    @Nonnull
    private static IFunctionDefinitionFragment.Parameter unlabeled(@Nonnull final String name,
                                                                   @Nonnull final IType type) {
        return new IFunctionDefinitionFragment.Parameter(null, name, type);
    }
}
