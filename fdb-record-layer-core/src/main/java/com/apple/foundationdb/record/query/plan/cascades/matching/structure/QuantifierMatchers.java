/*
 * QuantifierMatchers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.members;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithExtractAndDownstream.typedWithDownstream;

/**
 * Matchers for {@link Quantifier}s.
 */
@API(API.Status.EXPERIMENTAL)
public class QuantifierMatchers {
    private QuantifierMatchers() {
        // do not instantiate
    }

    public static <Q extends Quantifier> TypedMatcher<Q> ofType(@Nonnull final Class<Q> bindableClass) {
        return typed(bindableClass);
    }

    public static <Q extends Quantifier> BindingMatcher<Q> ofTypeRangingOver(@Nonnull final Class<Q> bindableClass,
                                                                             @Nonnull final BindingMatcher<? extends Collection<? extends RelationalExpression>> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.of(q -> q.getRangesOver().getAllMemberExpressions(), name -> "rangesOver(getAllMemberExpressions(" + name + "))"),
                downstream);
    }

    public static <Q extends Quantifier> BindingMatcher<Q> ofTypeRangingOverRef(@Nonnull final Class<Q> bindableClass,
                                                                                @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.of(Quantifier::getRangesOver, name -> "rangesOver(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static BindingMatcher<Quantifier> anyQuantifier() {
        return ofTypeRangingOverRef(Quantifier.class, ReferenceMatchers.anyRef());
    }

    @Nonnull
    public static BindingMatcher<Quantifier> anyQuantifier(@Nonnull final BindingMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<Quantifier> anyQuantifier(@Nonnull final CollectionMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.class, downstream);
    }

    public static BindingMatcher<Quantifier> anyQuantifierOverRef(@Nonnull final BindingMatcher<? extends Reference> downstream) {
        return ofTypeRangingOverRef(Quantifier.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Existential> existentialQuantifier(@Nonnull final BindingMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.Existential.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Existential> existentialQuantifier(@Nonnull final CollectionMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.Existential.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Existential> existentialQuantifier() {
        return ofTypeRangingOverRef(Quantifier.Existential.class, ReferenceMatchers.anyRef());
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Existential> existentialQuantifierOverRef(@Nonnull final BindingMatcher<? extends Reference> downstream) {
        return ofTypeRangingOverRef(Quantifier.Existential.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifier() {
        return ofTypeRangingOverRef(Quantifier.ForEach.class, ReferenceMatchers.anyRef());
    }


    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifier(@Nonnull final BindingMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.ForEach.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifier(@Nonnull final CollectionMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.ForEach.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifierWithDefaultOnEmpty() {
        return typedWithDownstream(Quantifier.ForEach.class,
                Extractor.of(Quantifier.ForEach::isNullOnEmpty, name -> "withDefaultOnEmpty(" + name + ")"),
                PrimitiveMatchers.equalsObject(true));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifierOverRef(@Nonnull final BindingMatcher<? extends Reference> downstream) {
        return ofTypeRangingOverRef(Quantifier.ForEach.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifierOverRef(@Nonnull final BindingMatcher<? extends Reference> downstream, BindingMatcher<? super Boolean> defaultOnEmptyMatcher) {
        return typedWithDownstream(Quantifier.ForEach.class,
                Extractor.identity(),
                AllOfMatcher.matchingAllOf(Quantifier.ForEach.class,
                        ImmutableList.of(
                                typedWithDownstream(Quantifier.ForEach.class,
                                        Extractor.of(Quantifier.ForEach::isNullOnEmpty, name -> "withDefaultOnEmpty(" + name + ")"),
                                        defaultOnEmptyMatcher
                                ),
                                typedWithDownstream(Quantifier.ForEach.class,
                                        Extractor.of(Quantifier::getRangesOver, name -> "rangesOver(" + name + ")"),
                                        downstream)
                        )));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifierWithDefaultOnEmptyOverRef(@Nonnull final BindingMatcher<? extends Reference> downstream) {
        return forEachQuantifierOverRef(downstream, PrimitiveMatchers.equalsObject(true));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifierWithoutDefaultOnEmptyOverRef(@Nonnull final BindingMatcher<? extends Reference> downstream) {
        return forEachQuantifierOverRef(downstream, PrimitiveMatchers.equalsObject(false));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifierOverPlans(@Nonnull final CollectionMatcher<RecordQueryPlan> downstream) {
        return forEachQuantifierOverRef(members(downstream));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Physical> physicalQuantifier() {
        return ofTypeRangingOverRef(Quantifier.Physical.class, ReferenceMatchers.anyRef());
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Physical> physicalQuantifier(@Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return ofTypeRangingOver(Quantifier.Physical.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Physical> physicalQuantifier(@Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return ofTypeRangingOver(Quantifier.Physical.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Physical> physicalQuantifierOverRef(@Nonnull final BindingMatcher<? extends Reference> downstream) {
        return ofTypeRangingOverRef(Quantifier.Physical.class, downstream);
    }
}
