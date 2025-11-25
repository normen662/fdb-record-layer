/*
 * RewritingCostModel.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.costing;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionCountProperty.selectCount;
import static com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionCountProperty.tableFunctionCount;
import static com.apple.foundationdb.record.query.plan.cascades.properties.PredicateComplexityProperty.predicateComplexity;
import static com.apple.foundationdb.record.query.plan.cascades.properties.PredicateHeightProperty.predicateHeight;

/**
 * Cost model for {@link PlannerPhase#REWRITING}. TODO To be fleshed out whe we have actual rules.
 */
@API(API.Status.EXPERIMENTAL)
@SpotBugsSuppressWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
public class RewritingCostModel implements CascadesCostModel<RelationalExpression> {
    @Nonnull
    private final RecordQueryPlannerConfiguration configuration;

    public RewritingCostModel(@Nonnull final RecordQueryPlannerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Nonnull
    @Override
    public RecordQueryPlannerConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public int compare(final RelationalExpression a, final RelationalExpression b) {
        //
        // Choose the expression with the fewest select boxes
        //
        final int aSelects = selectCount().evaluate(a);
        final int bSelects = selectCount().evaluate(b);
        if (aSelects != bSelects) {
            return Integer.compare(aSelects, bSelects);
        }

        //
        // Choose the expression with the fewest TableFunction expressions
        //
        final int aTableFunctions = tableFunctionCount().evaluate(a);
        final int bTableFunctions = tableFunctionCount().evaluate(b);
        if (aTableFunctions != bTableFunctions) {
            return Integer.compare(aTableFunctions, bTableFunctions);
        }

        //
        // Pick the expression where predicates have been pushed down as far as they can go
        //
        final int aPredicateHeight = predicateHeight().evaluate(a);
        final int bPredicateHeight = predicateHeight().evaluate(b);
        if (aPredicateHeight != bPredicateHeight) {
            return Integer.compare(aPredicateHeight, bPredicateHeight);
        }

        //
        // Choose the expression with the simplest predicate.
        //
        final int aPredicateComplexity = predicateComplexity().evaluate(a);
        final int bPredicateComplexity = predicateComplexity().evaluate(b);
        if (aPredicateComplexity != bPredicateComplexity) {
            return Integer.compare(aPredicateComplexity, bPredicateComplexity);
        }

        //
        // If expressions are indistinguishable from a cost perspective, select one by its semanticHash.
        //
        final int aSemanticHash = a.semanticHashCode();
        final int bSemanticHash = b.semanticHashCode();
        if (aSemanticHash != bSemanticHash) {
            return Integer.compare(aSemanticHash, bSemanticHash);
        }

        //
        // There can be duplicates. We must be sure to tie-break them as well.
        //
        return -1;
    }
}
