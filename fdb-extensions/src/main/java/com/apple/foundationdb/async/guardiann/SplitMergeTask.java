/*
 * SplitMergeTask.java
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.common.StorageHelpers;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.async.hnsw.ResultEntry;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.apple.foundationdb.async.MoreAsyncUtil.forEach;

public class SplitMergeTask extends AbstractDeferredTask {
    @Nonnull
    private final UUID clusterId;
    @Nonnull
    private final Transformed<RealVector> centroid;

    private SplitMergeTask(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                           @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                           @Nonnull final Transformed<RealVector> centroid) {
        super(locator, accessInfo, taskId);
        this.clusterId = clusterId;
        this.centroid = centroid;
    }

    @Nonnull
    public UUID getClusterId() {
        return clusterId;
    }

    @Nonnull
    public Transformed<RealVector> getCentroid() {
        return centroid;
    }

    @Nonnull
    @Override
    public Tuple valueTuple() {
        final Quantizer quantizer = getLocator().primitives().quantizer(getAccessInfo());
        final Transformed<RealVector> encodedVector = quantizer.encode(getCentroid());
        return Tuple.from(getKind().getCode(), clusterId, encodedVector.getUnderlyingVector().getRawData());
    }

    @Nonnull
    public CompletableFuture<Void> runTask(@Nonnull final Transaction transaction) {
        final Config config = getLocator().getConfig();
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();

        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final RealVector untransformedCentroid = storageTransform.untransform(getCentroid());
        final Quantizer quantizer = primitives.quantizer(accessInfo);

        return primitives.fetchClusterInfo(transaction, getClusterId())
                .thenCompose(clusterInfo -> {
                    if (clusterInfo == null || clusterInfo.getState() != ClusterInfo.State.SPLIT_MERGE) {
                        return AsyncUtil.DONE;
                    }

                    if (clusterInfo.getNumVectors() >= config.getClusterMin() ||
                            clusterInfo.getNumVectors() <= config.getClusterMax()) {
                        // false alarm
                        primitives.writeClusterInfo(transaction, new ClusterInfo(clusterInfo.getId(),
                                clusterInfo.getNumVectors(), ClusterInfo.State.ACTIVE));
                        return AsyncUtil.DONE;
                    }

                    if (clusterInfo.getNumVectors() > config.getClusterMax()) {
                        return split(transaction, clusterInfo, untransformedCentroid);
                    } else {
                        Verify.verify(clusterInfo.getNumVectors() < config.getClusterMin());
                        return merge(transaction, clusterInfo);
                    }
                });
    }

    @Nonnull
    public Kind getKind() {
        return Kind.SPLIT_MERGE;
    }

    @Nonnull
    private CompletableFuture<Void> merge(@Nonnull final Transaction transaction,
                                          @Nonnull final ClusterInfo clusterInfo) {
        return AsyncUtil.DONE;
    }

    @Nonnull
    private CompletableFuture<Void> split(@Nonnull final Transaction transaction,
                                          @Nonnull final ClusterInfo primaryClusterInfo,
                                          @Nonnull final RealVector primaryClusterCentroid) {
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();
        final int numInnerNeighborhood = 2;
        final int numOuterNeighborhood = 3;

        final var clusterNeighborhood =
                AsyncUtil.collect(
                        MoreAsyncUtil.mapIterablePipelined(executor,
                                MoreAsyncUtil.limitIterable(MoreAsyncUtil.iterableOf(() ->
                                                        primitives.centroidsOrderedByDistance(transaction, primaryClusterCentroid),
                                                executor),
                                        numInnerNeighborhood + numOuterNeighborhood, executor),
                                resultEntry ->
                                        primitives.fetchClusterInfo(transaction,
                                                StorageAdapter.clusterIdFromTuple(resultEntry.getPrimaryKey())), 10));
        clusterNeighborhood.thenCompose(clusterInfos -> {
            //
            // Not having the primary cluster in the neighborhood should be next to impossible. It can happen, however,
            // and we need to build for that rare corner case. Here we look for the primary cluster in the cluster
            // neighborhood and adjust the inner and outer neighborhood accordingly. Also log, if we cannot find the
            // primary cluster as that should be almost indicative of another problem.
            //
            boolean foundPrimaryCluster = false;
            for (final ClusterInfo clusterInfo : clusterInfos) {
                if (clusterInfo.getId().equals(primaryClusterInfo.getId())) {
                    foundPrimaryCluster = true;
                    break;
                }
            }

            final List<ClusterInfo> innerNeighborhood;
            final List<ClusterInfo> outerNeighborhood;
            if (foundPrimaryCluster) {
                innerNeighborhood = clusterInfos.subList(0, numInnerNeighborhood);
                outerNeighborhood = clusterInfos.subList(numInnerNeighborhood, clusterInfos.size());
            } else {
                final ImmutableList.Builder<ClusterInfo> innerNeighborhoodBuilder = ImmutableList.builder();
                innerNeighborhoodBuilder.add(primaryClusterInfo);
                innerNeighborhoodBuilder.addAll(clusterInfos.subList(0, numInnerNeighborhood - 1));
                innerNeighborhood = innerNeighborhoodBuilder.build();
                outerNeighborhood = clusterInfos.subList(numInnerNeighborhood - 1, clusterInfos.size() - 1);
            }

            //
            // At this point innerNeighborhood is comprised of the clusters we want to split into
            // innerNeighborhood.size() + 1 number of clusters and outerNeighborhood is comprised of all clusters we
            // may assign some vectors for innerNeighborhood to.
            //
        });

        return AsyncUtil.DONE;
    }

    @Nonnull
    static SplitMergeTask fromTuples(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                                     @Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        Verify.verify(Kind.fromValueTuple(valueTuple) == Kind.SPLIT_MERGE);
        final StorageTransform storageTransform = locator.primitives().storageTransform(accessInfo);
        final Transformed<RealVector> centroid = storageTransform.transform(
                StorageHelpers.vectorFromBytes(locator.getConfig(), valueTuple.getBytes(2)));

        return new SplitMergeTask(locator, accessInfo, keyTuple.getUUID(1),
                valueTuple.getUUID(1), centroid);
    }

    @Nonnull
    static SplitMergeTask of(@Nonnull final Locator locator, @Nonnull final AccessInfo accessInfo,
                             @Nonnull final UUID taskId, @Nonnull final UUID clusterId,
                             @Nonnull final Transformed<RealVector> centroid) {
        return new SplitMergeTask(locator, accessInfo, taskId, clusterId, centroid);
    }
}
