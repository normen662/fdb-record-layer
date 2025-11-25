/*
 * CascadesCostModel.java
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
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Basic cost model interface to be provided by each {@link PlannerPhase}.
 */
@API(API.Status.EXPERIMENTAL)
public interface CascadesCostModel extends Comparator<RelationalExpression> {
    @Nonnull
    RecordQueryPlannerConfiguration getConfiguration();

    @Nonnull
    static Collector<RelationalExpression, LinkedIdentitySet<RelationalExpression>, Set<RelationalExpression>>
             toBestExpressions(@Nonnull final CascadesCostModel costModel) {
        return toBestExpressions(costModel,
                expression -> {
                    // nothing
                });
    }

    @Nonnull
    static Collector<RelationalExpression, LinkedIdentitySet<RelationalExpression>, Set<RelationalExpression>>
             toBestExpressions(@Nonnull final CascadesCostModel costModel,
                               @Nonnull final Consumer<RelationalExpression> onRemoveConsumer) {
        return new BestExpressionsCollector<>(
                ImmutableSet.copyOf(EnumSet.of(Collector.Characteristics.UNORDERED,
                        Collector.Characteristics.IDENTITY_FINISH)),
                costModel, onRemoveConsumer);
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
        private final CascadesCostModel costModel;
        @Nonnull
        private final Consumer<T> onRemoveConsumer;

        private BestExpressionsCollector(@Nonnull final Set<Characteristics> characteristics,
                                         @Nonnull final CascadesCostModel costModel,
                                         @Nonnull final Consumer<T> onRemoveConsumer) {
            this.characteristics = characteristics;
            this.costModel = costModel;
            this.onRemoveConsumer = onRemoveConsumer;
        }

        @Override
        public BiConsumer<LinkedIdentitySet<T>, T> accumulator() {
            return (bestExpressions, newExpression) -> {
                // pick a representative from the best expressions set and cost that against the new expression
                final var aBestExpression = Iterables.getFirst(bestExpressions, null);
                final var compare =
                        aBestExpression == null ? -1 : costModel.compare(newExpression, aBestExpression);
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
                final var compare = costModel.compare(aLeftBestExpression, aRightBestExpression);
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
