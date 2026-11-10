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

import com.apple.foundationdb.record.ir.types.IGenericType;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link Scope}: what a scope declares is visible there and in the scopes it encloses, and leaving a scope
 * drops what it declared.
 */
class ScopeTest {
    private static final IGenericType T = new IGenericType("T", false);
    private static final IGenericType U = new IGenericType("U", false);

    @Test
    void lookupTypeParameterDeclaredInEnclosingScopeFindsDeclaration() {
        final Scope outer = Scope.root().enter(ImmutableList.of(T));
        final Scope inner = outer.enter(ImmutableList.of(U));
        assertThat(inner.lookupTypeParameter("T")).containsSame(T);
        assertThat(inner.lookupTypeParameter("U")).containsSame(U);
        assertThat(outer.lookupTypeParameter("U")).isEmpty();
        assertThat(inner.exit().lookupTypeParameter("U")).isEmpty();
    }

    @Test
    void lookupTypeParameterInnerDeclarationHidesOuterOne() {
        final IGenericType outerT = new IGenericType("T", false);
        final IGenericType innerT = new IGenericType("T", false);
        final Scope inner = Scope.root().enter(ImmutableList.of(outerT)).enter(ImmutableList.of(innerT));
        assertThat(inner.lookupTypeParameter("T")).containsSame(innerT);
        assertThat(inner.exit().lookupTypeParameter("T")).containsSame(outerT);
    }

    @Test
    void enterDuplicateTypeParameterFails() {
        assertThatThrownBy(() -> Scope.root().enter(ImmutableList.of(T, new IGenericType("T", false))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void isFunctionDeclaredInEnclosingScopeIsVisible() {
        final Scope outer = Scope.root();
        outer.declareFunction("f");
        final Scope inner = outer.enter(ImmutableList.of());
        inner.declareFunction("g");
        assertThat(inner.isFunction("f")).isTrue();
        assertThat(inner.isFunction("g")).isTrue();
        assertThat(outer.isFunction("g")).isFalse();
        assertThat(inner.exit().isFunction("g")).isFalse();
    }

    @Test
    void lookupTypeParameterFunctionNameIsNotTypeParameter() {
        final Scope scope = Scope.root();
        scope.declareFunction("f");
        assertThat(scope.lookupTypeParameter("f")).isEmpty();
        assertThat(scope.enter(ImmutableList.of(T)).isFunction("T")).isFalse();
    }

    @Test
    void exitOutermostScopeFails() {
        assertThatThrownBy(() -> Scope.root().exit()).isInstanceOf(IllegalStateException.class);
    }
}
