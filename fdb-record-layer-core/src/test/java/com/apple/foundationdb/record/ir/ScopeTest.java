/*
 * ScopeTest.java
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

import com.apple.foundationdb.record.ir.fragments.IExternType;
import com.apple.foundationdb.record.ir.fragments.IFunctionDeclarationFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionDefinitionFragment;
import com.apple.foundationdb.record.ir.fragments.IFunctionFragment;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.apple.foundationdb.record.ir.fragments.IOverloadedFunctionFragment;
import com.apple.foundationdb.record.ir.fragments.IPackageStatementFragment;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.fragments.ITypeAliasFragment;
import com.apple.foundationdb.record.ir.fragments.ITypeDeclarationFragment;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link Scope}: what a scope declares is visible there and in the scopes it encloses, leaving a scope drops
 * what it declared, and a scope carries the inference that types its expressions, the package of the program, and the
 * element type of the stream that a yield adds to. An overloaded function collects the variants that its scope
 * declares after it, and operators are scoped as names are.
 */
class ScopeTest {
    private static final IGenericType T = new IGenericType("T", false);
    private static final IGenericType U = new IGenericType("U", false);
    private static final IType INT = new IPrimitiveType(IPrimitiveType.Kind.INT, false);
    private static final IType STRING = new IPrimitiveType(IPrimitiveType.Kind.STRING, false);
    private static final TypeInference.Term INT_TERM = TypeInference.known(INT);
    private static final TypeInference.Term STRING_TERM = TypeInference.known(STRING);
    private static final IFunctionDeclarationFragment IDENTITY = function("f", T);
    private static final ITypeDeclarationFragment RANGE =
            new ITypeDeclarationFragment("range", ImmutableList.of(), null);
    private static final IOverloadedFunctionFragment OVERLOADED_ID = new IOverloadedFunctionFragment("id",
            ImmutableList.of(T), ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", T)),
            ImmutableList.of());

    @Test
    void lookupTypeParameterDeclaredInEnclosingScopeFindsDeclaration() {
        final Scope outer = Scope.root().enterFunction(ImmutableList.of(T));
        final Scope inner = outer.enterFunction(ImmutableList.of(U));
        assertThat(inner.lookupTypeParameter("T")).containsSame(T);
        assertThat(inner.lookupTypeParameter("U")).containsSame(U);
        assertThat(outer.lookupTypeParameter("U")).isEmpty();
        assertThat(inner.exit().lookupTypeParameter("U")).isEmpty();
    }

    @Test
    void enterFunctionTypeParameterOfEnclosingScopeFails() {
        // type parameters compare by name, so an inner T would be the same type as the outer one
        final Scope outer = Scope.root().enterFunction(ImmutableList.of(T));
        assertThatThrownBy(() -> outer.enterFunction(ImmutableList.of(new IGenericType("T", false))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> outer.enterBlock().enterFunction(ImmutableList.of(new IGenericType("T", false))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void enterFunctionDuplicateTypeParameterFails() {
        assertThatThrownBy(() -> Scope.root().enterFunction(ImmutableList.of(T, new IGenericType("T", false))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lookupTypeFindsTypeParametersAndExternTypes() {
        final Scope outer = Scope.root();
        outer.declareExternType(RANGE);
        final Scope inner = outer.enterFunction(ImmutableList.of(T)).enterBlock();
        // an extern type is the type of its declaration, which a lookup returns without nullability
        assertThat(inner.lookupType("range")).contains(new IExternType(RANGE, false));
        assertThat(inner.lookupType("T")).containsSame(T);
        assertThat(inner.lookupType("U")).isEmpty();
        assertThat(outer.lookupType("T")).isEmpty();
    }

    @Test
    void declareExternTypeInnerDeclarationHidesOuterTypes() {
        final ITypeDeclarationFragment innerT = new ITypeDeclarationFragment("T", ImmutableList.of(), null);
        final Scope function = Scope.root().enterFunction(ImmutableList.of(T));
        final Scope body = function.enterBlock();
        body.declareExternType(innerT);
        // an extern type refers to its declaration, so it can hide a type parameter without ambiguity
        assertThat(body.lookupType("T")).contains(new IExternType(innerT, false));
        assertThat(body.exit().lookupType("T")).containsSame(T);
    }

    @Test
    void declareExternTypeTwiceInOneScopeFails() {
        final Scope scope = Scope.root();
        scope.declareExternType(RANGE);
        assertThatThrownBy(() -> scope.declareExternType(
                new ITypeDeclarationFragment("range", ImmutableList.of(), null)))
                .isInstanceOf(IllegalArgumentException.class);
        // nor can a scope declare an extern type by the name of one of its type parameters
        final Scope function = Scope.root().enterFunction(ImmutableList.of(T));
        assertThatThrownBy(() -> function.declareExternType(
                new ITypeDeclarationFragment("T", ImmutableList.of(), null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lookupTypeFindsTypeOfTypeAlias() {
        final IType nullableString = STRING.withNullability(true);
        final Scope outer = Scope.root();
        outer.declareTypeAlias(new ITypeAliasFragment("name", nullableString));
        final Scope inner = outer.enterFunction(ImmutableList.of(T)).enterBlock();
        // a type alias stands for its type, which can be nullable
        assertThat(inner.lookupType("name")).contains(nullableString);
        // an alias can hide a type parameter, which the lookup of type parameters still finds
        inner.declareTypeAlias(new ITypeAliasFragment("T", INT));
        assertThat(inner.lookupType("T")).contains(INT);
        assertThat(inner.lookupTypeParameter("T")).containsSame(T);
        assertThat(inner.exit().lookupType("T")).containsSame(T);
    }

    @Test
    void declareTypeAliasNameOfTypeInOneScopeFails() {
        // extern types and type aliases share their names in a scope
        final Scope scope = Scope.root();
        scope.declareExternType(RANGE);
        scope.declareTypeAlias(new ITypeAliasFragment("row", INT));
        assertThatThrownBy(() -> scope.declareTypeAlias(new ITypeAliasFragment("range", INT)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scope.declareTypeAlias(new ITypeAliasFragment("row", STRING)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scope.declareExternType(new ITypeDeclarationFragment("row", ImmutableList.of(), null)))
                .isInstanceOf(IllegalArgumentException.class);
        // nor can a scope declare an alias by the name of one of its type parameters
        final Scope function = Scope.root().enterFunction(ImmutableList.of(T));
        assertThatThrownBy(() -> function.declareTypeAlias(new ITypeAliasFragment("T", INT)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lookupValueDeclaredInEnclosingScopeFindsDeclaration() {
        final Scope outer = Scope.root();
        outer.declareFunction(IDENTITY);
        final Scope inner = outer.enterBlock();
        inner.declareVariable("x", INT_TERM);
        // a function is held by its declaration, which a reference to the function refers to
        assertThat(inner.lookupValue("f")).contains(new Scope.FunctionBinding(IDENTITY));
        assertThat(inner.lookupValue("x")).contains(new Scope.VariableBinding(INT_TERM, null));
        assertThat(outer.lookupValue("x")).isEmpty();
        assertThat(inner.exit().lookupValue("x")).isEmpty();
    }

    @Test
    void lookupValueInnerDeclarationHidesOuterOne() {
        final Scope outer = Scope.root();
        outer.declareVariable("x", INT_TERM);
        final Scope inner = outer.enterBlock();
        inner.declareVariable("x", STRING_TERM);
        assertThat(inner.lookupValue("x")).contains(new Scope.VariableBinding(STRING_TERM, null));
        assertThat(inner.exit().lookupValue("x")).contains(new Scope.VariableBinding(INT_TERM, null));
    }

    @Test
    void lookupFunctionSkipsVariablesOfName() {
        final Scope outer = Scope.root();
        outer.declareFunction(IDENTITY);
        final Scope inner = outer.enterBlock();
        inner.declareVariable("f", INT_TERM);
        // a dot call calls a function, so a variable of the name hides the function only from other lookups
        assertThat(inner.lookupFunction("f")).contains(new Scope.FunctionBinding(IDENTITY));
        assertThat(inner.lookupValue("f")).contains(new Scope.VariableBinding(INT_TERM, null));
        assertThat(Scope.root().lookupFunction("f")).isEmpty();
    }

    @Test
    void declareNameTwiceInOneScopeFails() {
        final Scope scope = Scope.root();
        scope.declareVariable("x", INT_TERM);
        scope.declareFunction(function("f", INT));
        // variables and functions share one namespace, and there is no overloading
        assertThatThrownBy(() -> scope.declareVariable("x", STRING_TERM)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scope.declareVariable("f", INT_TERM)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scope.declareFunction(function("x", T)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lookupTypeParameterValueIsNotTypeParameter() {
        // types and values have separate namespaces
        final Scope scope = Scope.root();
        scope.declareVariable("T", INT_TERM);
        scope.declareExternType(RANGE);
        scope.declareVariable("range", INT_TERM);
        assertThat(scope.lookupTypeParameter("T")).isEmpty();
        assertThat(scope.lookupType("range")).contains(new IExternType(RANGE, false));
        assertThat(scope.lookupValue("range")).contains(new Scope.VariableBinding(INT_TERM, null));
        assertThat(scope.enterFunction(ImmutableList.of(U)).lookupValue("U")).isEmpty();
    }

    @Test
    void inferenceClosureInheritsAndFunctionHasNone() {
        final TypeInference inference = new TypeInference();
        final Scope statement = Scope.root().enterStatement(inference);
        // the scope of a closure inherits the inference of its statement, the scope of a function has none
        assertThat(statement.enterClosure().inference()).isSameAs(inference);
        assertThat(statement.enterClosure().enterFunction(ImmutableList.of()).inference()).isNull();
        assertThat(statement.enterClosure().enterFunction(ImmutableList.of()).enterBlock().inference()).isNull();
        assertThat(Scope.root().inference()).isNull();
    }

    @Test
    void enterBlockWithoutInferenceStatementsOfBlockStartTheirOwn() {
        final TypeInference inference = new TypeInference();
        final Scope statement = Scope.root().enterStatement(inference);
        // a block of an if or a for joins the inference of its statement, unless that statement has an inference of
        // its own; the statements of the block then start their own
        assertThat(statement.enterBlock().inference()).isSameAs(inference);
        final Scope block = statement.enterBlockWithoutInference();
        assertThat(block.inference()).isNull();
        final TypeInference blockStatementInference = new TypeInference();
        assertThat(block.enterStatement(blockStatementInference).inference()).isSameAs(blockStatementInference);
        // the block runs as part of the block that its statement is in, so it keeps the element type of yields
        assertThat(statement.enterStream(INT_TERM).enterBlockWithoutInference().yieldElementType()).isSameAs(INT_TERM);
    }

    @Test
    void yieldElementTypeStreamSetsItAndClosuresAndFunctionsDropIt() {
        final TypeInference inference = new TypeInference();
        final Scope stream = Scope.root().enterStatement(inference).enterStream(INT_TERM);
        assertThat(stream.yieldElementType()).isSameAs(INT_TERM);
        // the statements of the stream block join the inference of its statement, as those of a closure do
        assertThat(stream.inference()).isSameAs(inference);
        // a block in the stream block inherits the element type, and a nested stream block has its own
        assertThat(stream.enterBlock().yieldElementType()).isSameAs(INT_TERM);
        assertThat(stream.enterStream(STRING_TERM).yieldElementType()).isSameAs(STRING_TERM);
        // a function or a closure can be called when the stream block is not running, so neither it nor its blocks
        // have an element type
        assertThat(stream.enterClosure().yieldElementType()).isNull();
        assertThat(stream.enterClosure().enterBlock().yieldElementType()).isNull();
        assertThat(stream.enterFunction(ImmutableList.of()).enterBlock().yieldElementType()).isNull();
        // a statement of a program is in no stream block
        assertThat(Scope.root().yieldElementType()).isNull();
        assertThat(Scope.root().enterStatement(new TypeInference()).yieldElementType()).isNull();
    }

    @Test
    void declareVariableInStatementScopeDeclaresInEnclosingScope() {
        final TypeInference inference = new TypeInference();
        final Scope block = Scope.root();
        final Scope statement = block.enterStatement(inference);
        statement.declareVariable("x", INT_TERM);
        statement.declareFunction(IDENTITY);
        statement.declareExternType(RANGE);
        statement.declareTypeAlias(new ITypeAliasFragment("row", INT));
        // what a statement declares is visible after it, and a variable belongs to the inference of its statement
        assertThat(statement.exit()).isSameAs(block);
        assertThat(block.lookupValue("x")).contains(new Scope.VariableBinding(INT_TERM, inference));
        assertThat(block.lookupValue("f")).isPresent();
        assertThat(block.lookupType("range")).isPresent();
        assertThat(block.lookupType("row")).contains(INT);
        // a closure's variable belongs to the inference of the closure's statement too
        final Scope closure = statement.enterClosure();
        closure.declareVariable("y", STRING_TERM);
        assertThat(closure.lookupValue("y")).contains(new Scope.VariableBinding(STRING_TERM, inference));
    }

    @Test
    void declarePackageHoldsPackageInOutermostScope() {
        final IPackageStatementFragment abc = new IPackageStatementFragment("abc", ImmutableList.of());
        final Scope root = Scope.root();
        final Scope statement = root.enterStatement(new TypeInference());
        statement.declarePackage(abc);
        // every scope of the program finds the package
        assertThat(root.packageStatement()).isSameAs(abc);
        assertThat(statement.enterBlock().enterFunction(ImmutableList.of(T)).packageStatement()).isSameAs(abc);
        assertThat(Scope.root().packageStatement()).isNull();
        // a program belongs to one package at most
        assertThatThrownBy(() -> root.declarePackage(new IPackageStatementFragment("def", ImmutableList.of())))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void enterStatementInStatementFails() {
        // a statement of a closure joins the inference of the closure's statement
        final Scope closure = Scope.root().enterStatement(new TypeInference()).enterClosure();
        assertThatThrownBy(() -> closure.enterStatement(new TypeInference())).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void exitOutermostScopeFails() {
        assertThatThrownBy(() -> Scope.root().exit()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void declareVariantAddsVariantToOverloadedFunctionOfScope() {
        final Scope scope = Scope.root();
        final Scope.OverloadedFunctionBinding binding = scope.declareOverloadedFunction(OVERLOADED_ID);
        final IFunctionDeclarationFragment intVariant = variant(INT);
        final IFunctionDeclarationFragment stringVariant = variant(STRING);
        scope.declareVariant(intVariant);
        // a call sees the variants declared before it, so it gets a copy of them
        final List<IFunctionFragment> beforeStringVariant = binding.variants();
        scope.declareVariant(stringVariant);
        assertThat(beforeStringVariant).containsExactly(intVariant);
        assertThat(binding.variants()).containsExactly(intVariant, stringVariant);
        assertThat(scope.lookupValue("id")).containsSame(binding);
        assertThat(scope.enterBlock().lookupFunction("id")).containsSame(binding);
        // the scope of a statement declares into the scope that encloses it
        final Scope statement = Scope.root().enterStatement(new TypeInference());
        final Scope.OverloadedFunctionBinding statementBinding = statement.declareOverloadedFunction(OVERLOADED_ID);
        statement.declareVariant(intVariant);
        assertThat(statement.overloadedFunction("id")).containsSame(statementBinding);
        assertThat(statement.exit().overloadedFunction("id")).containsSame(statementBinding);
        assertThat(statementBinding.variants()).containsExactly(intVariant);
    }

    @Test
    void declareVariantOutsideScopeOfOverloadedFunctionFails() {
        final Scope outer = Scope.root();
        outer.declareOverloadedFunction(OVERLOADED_ID);
        // a variant is declared after its overloaded function, in the same scope, and its key is its own
        assertThatThrownBy(() -> outer.enterBlock().declareVariant(variant(INT)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Scope.root().declareVariant(variant(INT)))
                .isInstanceOf(IllegalArgumentException.class);
        outer.declareVariant(variant(INT));
        assertThatThrownBy(() -> outer.declareVariant(variant(INT))).isInstanceOf(IllegalArgumentException.class);
        // a function that is not a variant is no variant
        assertThatThrownBy(() -> outer.declareVariant(function("id", INT)))
                .isInstanceOf(IllegalArgumentException.class);
        // the overloaded function has its name, so no variable or function of the scope can have it
        assertThatThrownBy(() -> outer.declareVariable("id", INT_TERM)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> outer.declareFunction(function("id", INT)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lookupOperatorInnerDeclarationHidesOuterOne() {
        final Scope outer = Scope.root();
        final Scope.FunctionBinding outerFunction = outer.declareFunction(function("plus", INT));
        outer.declareOperator(Operator.ADD, outerFunction);
        final Scope inner = outer.enterBlock();
        final Scope.OverloadedFunctionBinding innerFunction = inner.declareOverloadedFunction(OVERLOADED_ID);
        inner.declareOperator(Operator.ADD, innerFunction);
        assertThat(inner.lookupOperator(Operator.ADD)).containsSame(innerFunction);
        assertThat(inner.exit().lookupOperator(Operator.ADD)).containsSame(outerFunction);
        // operators have a namespace of their own, and the two operators with the symbol - are different
        assertThat(inner.lookupOperator(Operator.SUBTRACT)).isEmpty();
        outer.declareOperator(Operator.NEGATE, outerFunction);
        assertThat(inner.lookupOperator(Operator.SUBTRACT)).isEmpty();
        assertThat(inner.lookupOperator(Operator.NEGATE)).containsSame(outerFunction);
        // a scope declares an operator once at most
        assertThatThrownBy(() -> outer.declareOperator(Operator.ADD, outerFunction))
                .isInstanceOf(IllegalArgumentException.class);
        // the scope of a statement declares into the scope that encloses it
        final Scope statement = Scope.root().enterStatement(new TypeInference());
        statement.declareOperator(Operator.LESS, outerFunction);
        assertThat(statement.exit().lookupOperator(Operator.LESS)).containsSame(outerFunction);
    }

    @Test
    void ofOperatorSymbolAndOperandCountFindsOperator() {
        assertThat(Operator.of("-", 1)).contains(Operator.NEGATE);
        assertThat(Operator.of("-", 2)).contains(Operator.SUBTRACT);
        assertThat(Operator.of("!", 1)).contains(Operator.NOT);
        assertThat(Operator.of(">=", 2)).contains(Operator.GREATER_OR_EQUAL);
        // there is no prefix +, no binary !, and no operator && yet
        assertThat(Operator.of("+", 1)).isEmpty();
        assertThat(Operator.of("!", 2)).isEmpty();
        assertThat(Operator.of("&&", 2)).isEmpty();
        assertThat(Operator.of("-", 3)).isEmpty();
    }

    /**
     * Returns the declaration of a function with one parameter of the given type, which is its result type too, and
     * which is generic in {@code T} if the type is {@code T}.
     */
    @Nonnull
    private static IFunctionDeclarationFragment function(@Nonnull final String name, @Nonnull final IType type) {
        return new IFunctionDeclarationFragment(name, type.equals(T) ? ImmutableList.of(T) : ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", type)), type);
    }

    /**
     * Returns the variant of {@code overloaded func id<T>(x: T)} for the given type, which is its result type too.
     */
    @Nonnull
    private static IFunctionDeclarationFragment variant(@Nonnull final IType type) {
        return new IFunctionDeclarationFragment("id", ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinitionFragment.Parameter("x", type)), type, ImmutableList.of(), null,
                new IOverloadedFunctionFragment.Variant(OVERLOADED_ID, ImmutableList.of(type)));
    }
}
