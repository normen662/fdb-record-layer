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
import com.apple.foundationdb.record.ir.fragments.IFunctionDeclaration;
import com.apple.foundationdb.record.ir.fragments.IFunctionDefinition;
import com.apple.foundationdb.record.ir.fragments.IGenericType;
import com.apple.foundationdb.record.ir.fragments.IPackageStatement;
import com.apple.foundationdb.record.ir.fragments.IPrimitiveType;
import com.apple.foundationdb.record.ir.fragments.IType;
import com.apple.foundationdb.record.ir.fragments.ITypeDeclaration;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link Scope}: what a scope declares is visible there and in the scopes it encloses, leaving a scope drops
 * what it declared, and a scope carries the inference that types its expressions and the package of the program.
 */
class ScopeTest {
    private static final IGenericType T = new IGenericType("T", false);
    private static final IGenericType U = new IGenericType("U", false);
    private static final IType INT = new IPrimitiveType(IPrimitiveType.Kind.INT, false);
    private static final IType STRING = new IPrimitiveType(IPrimitiveType.Kind.STRING, false);
    private static final TypeInference.Term INT_TERM = TypeInference.known(INT);
    private static final TypeInference.Term STRING_TERM = TypeInference.known(STRING);
    private static final IFunctionDeclaration IDENTITY = function("f", T);
    private static final ITypeDeclaration RANGE = new ITypeDeclaration("range", ImmutableList.of(), null);

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
        final ITypeDeclaration innerT = new ITypeDeclaration("T", ImmutableList.of(), null);
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
        assertThatThrownBy(() -> scope.declareExternType(new ITypeDeclaration("range", ImmutableList.of(), null)))
                .isInstanceOf(IllegalArgumentException.class);
        // nor can a scope declare an extern type by the name of one of its type parameters
        final Scope function = Scope.root().enterFunction(ImmutableList.of(T));
        assertThatThrownBy(() -> function.declareExternType(new ITypeDeclaration("T", ImmutableList.of(), null)))
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
    void inferenceBlockInheritsAndFunctionHasNone() {
        final TypeInference inference = new TypeInference();
        final Scope statement = Scope.root().enterStatement(inference);
        // the scope of a closure inherits the inference of its statement, the scope of a function has none
        assertThat(statement.enterBlock().inference()).isSameAs(inference);
        assertThat(statement.enterBlock().enterFunction(ImmutableList.of()).inference()).isNull();
        assertThat(statement.enterBlock().enterFunction(ImmutableList.of()).enterBlock().inference()).isNull();
        assertThat(Scope.root().inference()).isNull();
    }

    @Test
    void declareVariableInStatementScopeDeclaresInEnclosingScope() {
        final TypeInference inference = new TypeInference();
        final Scope block = Scope.root();
        final Scope statement = block.enterStatement(inference);
        statement.declareVariable("x", INT_TERM);
        statement.declareFunction(IDENTITY);
        statement.declareExternType(RANGE);
        // what a statement declares is visible after it, and a variable belongs to the inference of its statement
        assertThat(statement.exit()).isSameAs(block);
        assertThat(block.lookupValue("x")).contains(new Scope.VariableBinding(INT_TERM, inference));
        assertThat(block.lookupValue("f")).isPresent();
        assertThat(block.lookupType("range")).isPresent();
        // a closure's variable belongs to the inference of the closure's statement too
        final Scope closure = statement.enterBlock();
        closure.declareVariable("y", STRING_TERM);
        assertThat(closure.lookupValue("y")).contains(new Scope.VariableBinding(STRING_TERM, inference));
    }

    @Test
    void declarePackageHoldsPackageInOutermostScope() {
        final IPackageStatement abc = new IPackageStatement("abc", ImmutableList.of());
        final Scope root = Scope.root();
        final Scope statement = root.enterStatement(new TypeInference());
        statement.declarePackage(abc);
        // every scope of the program finds the package
        assertThat(root.packageStatement()).isSameAs(abc);
        assertThat(statement.enterBlock().enterFunction(ImmutableList.of(T)).packageStatement()).isSameAs(abc);
        assertThat(Scope.root().packageStatement()).isNull();
        // a program belongs to one package at most
        assertThatThrownBy(() -> root.declarePackage(new IPackageStatement("def", ImmutableList.of())))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void enterStatementInStatementFails() {
        // a statement of a closure joins the inference of the closure's statement
        final Scope closure = Scope.root().enterStatement(new TypeInference()).enterBlock();
        assertThatThrownBy(() -> closure.enterStatement(new TypeInference())).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void exitOutermostScopeFails() {
        assertThatThrownBy(() -> Scope.root().exit()).isInstanceOf(IllegalStateException.class);
    }

    /**
     * Returns the declaration of a function with one parameter of the given type, which is its result type too, and
     * which is generic in {@code T} if the type is {@code T}.
     */
    @Nonnull
    private static IFunctionDeclaration function(@Nonnull final String name, @Nonnull final IType type) {
        return new IFunctionDeclaration(name, type.equals(T) ? ImmutableList.of(T) : ImmutableList.of(),
                ImmutableList.of(new IFunctionDefinition.Parameter("x", type)), type);
    }
}
