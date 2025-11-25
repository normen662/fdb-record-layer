/*
 * Tiebreaker.java
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

import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

interface Tiebreaker<T extends RelationalExpression> {
    int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                @Nonnull final T a, @Nonnull final T b);

    @Nonnull
    static <T extends RelationalExpression> TiebreakerResult<T>
            breakIfTied(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration,
                        @Nonnull final Tiebreaker<T> tieBreaker,
                        @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                        @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                        @Nonnull final Set<T> expressions,
                        @Nonnull final Consumer<T> onRemoveConsumer) {
        if (expressions.size() <= 1) {
            return new TerminalTiebreaker<>(expressions);
        }
        final var bestExpressions =
                expressions.stream()
                        .collect(toBestExpressions(plannerConfiguration, tieBreaker, opsMapA, opsMapB, onRemoveConsumer));

        return new TiebreakerResultWithNext<>(plannerConfiguration, opsMapA, opsMapB, bestExpressions, onRemoveConsumer);
    }

    @Nonnull
    static <T extends RelationalExpression> Collector<T, LinkedIdentitySet<T>, Set<T>>
             toBestExpressions(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration,
                               @Nonnull final Tiebreaker<T> tieBreaker,
                               @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                               @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                               @Nonnull final Consumer<T> onRemoveConsumer) {
        return new BestExpressionsCollector<>(
                ImmutableSet.copyOf(EnumSet.of(Collector.Characteristics.UNORDERED,
                        Collector.Characteristics.IDENTITY_FINISH)),
                plannerConfiguration, tieBreaker, opsMapA, opsMapB, onRemoveConsumer);
    }

    /**
     * Simple implementation class for {@code Collector}.
     *
     * @param <T> the type of elements to be collected
     */
    class BestExpressionsCollector<T extends RelationalExpression> implements Collector<T, LinkedIdentitySet<T>, Set<T>> {
        @Nonnull
        private final Set<Characteristics> characteristics;
        @Nonnull
        private final RecordQueryPlannerConfiguration plannerConfiguration;
        @Nonnull
        private final Tiebreaker<T> tieBreaker;
        @Nonnull
        private final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA;
        @Nonnull
        private final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB;
        @Nonnull
        private final Consumer<T> onRemoveConsumer;

        private BestExpressionsCollector(@Nonnull final Set<Characteristics> characteristics,
                                         @Nonnull final RecordQueryPlannerConfiguration plannerConfiguration,
                                         @Nonnull final Tiebreaker<T> tieBreaker,
                                         @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                                         @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                                         @Nonnull final Consumer<T> onRemoveConsumer) {
            this.characteristics = characteristics;
            this.plannerConfiguration = plannerConfiguration;
            this.tieBreaker = tieBreaker;
            this.opsMapA = opsMapA;
            this.opsMapB = opsMapB;
            this.onRemoveConsumer = onRemoveConsumer;
        }

        @Override
        public BiConsumer<LinkedIdentitySet<T>, T> accumulator() {
            return (bestExpressions, newExpression) -> {
                // pick a representative from the best expressions set and cost that against the new expression
                final var aBestExpression = Iterables.getFirst(bestExpressions, null);
                final var compare =
                        aBestExpression == null
                        ? -1
                        : tieBreaker.compare(plannerConfiguration, opsMapA, opsMapB,
                                newExpression, aBestExpression);
                if (compare < 0) {
                    bestExpressions.clear();
                    bestExpressions.add(newExpression);
                } else if (compare == 0) {
                    bestExpressions.add(newExpression);
                }
                //
                // Note, that if expression is more costly than bestExpressions, it will be dropped.
                //
                onRemoveConsumer.accept(newExpression);
            };
        }

        @Override
        public Supplier<LinkedIdentitySet<T>> supplier() {
            return LinkedIdentitySet::new;
        }

        @Override
        public BinaryOperator<LinkedIdentitySet<T>> combiner() {
            return (left, right) -> {
                final var aLeftBestExpression = Iterables.getFirst(left, null);
                final var aRightBestExpression = Iterables.getFirst(left, null);
                if (aLeftBestExpression == null) {
                    return right;
                }
                if (aRightBestExpression == null) {
                    return left;
                }
                final var compare = tieBreaker.compare(plannerConfiguration, opsMapA, opsMapB,
                        aLeftBestExpression, aRightBestExpression);
                if (compare < 0) {
                    right.forEach(onRemoveConsumer);
                    return left;
                } else if (compare > 0) {
                    left.forEach(onRemoveConsumer);
                    return right;
                }
                return new LinkedIdentitySet<>(Iterables.concat(left, right));
            };
        }

        @Override
        public Function<LinkedIdentitySet<T>, Set<T>> finisher() {
            return s -> s;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return characteristics;
        }
    }
}
