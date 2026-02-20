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
import com.apple.foundationdb.async.common.RandomHelpers;
import com.apple.foundationdb.async.common.StorageHelpers;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.kmeans.BoundedKMeans;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.Lens;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.apple.foundationdb.async.MoreAsyncUtil.forEach;

public class SplitMergeTask extends AbstractDeferredTask {
    private final static Lens<VectorReference, RealVector> vectorReferenceVectorLens =
            new VectorReferenceVectorLens().compose(Transformed.underlyingLens());

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

        return primitives.fetchClusterMetadata(transaction, getClusterId())
                .thenCompose(clusterMetadata -> {
                    if (clusterMetadata == null || clusterMetadata.getState() != ClusterMetadata.State.SPLIT_MERGE) {
                        return AsyncUtil.DONE;
                    }

                    if (clusterMetadata.getNumVectors() >= config.getClusterMin() ||
                            clusterMetadata.getNumVectors() <= config.getClusterMax()) {
                        // false alarm
                        primitives.writeClusterMetadata(transaction, new ClusterMetadata(clusterMetadata.getId(),
                                clusterMetadata.getNumVectors(), ClusterMetadata.State.ACTIVE));
                        return AsyncUtil.DONE;
                    }

                    if (clusterMetadata.getNumVectors() > config.getClusterMax()) {
                        return split(transaction, clusterMetadata, untransformedCentroid);
                    } else {
                        Verify.verify(clusterMetadata.getNumVectors() < config.getClusterMin());
                        return merge(transaction, clusterMetadata);
                    }
                });
    }

    @Nonnull
    public Kind getKind() {
        return Kind.SPLIT_MERGE;
    }

    @Nonnull
    private CompletableFuture<Void> merge(@Nonnull final Transaction transaction,
                                          @Nonnull final ClusterMetadata clusterMetadata) {
        return AsyncUtil.DONE;
    }

    @Nonnull
    private CompletableFuture<Void> split(@Nonnull final Transaction transaction,
                                          @Nonnull final ClusterMetadata primaryClusterMetadata,
                                          @Nonnull final RealVector primaryClusterCentroid) {
        final SplittableRandom random = RandomHelpers.random(primaryClusterMetadata.getId());
        final Primitives primitives = getLocator().primitives();
        final Executor executor = getLocator().getExecutor();
        final AccessInfo accessInfo = getAccessInfo();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
        final Quantizer quantizer = primitives.quantizer(accessInfo);
        final Estimator estimator = quantizer.estimator();

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
                                        primitives.fetchClusterMetadataWithDistance(transaction,
                                                StorageAdapter.clusterIdFromTuple(resultEntry.getPrimaryKey()),
                                                storageTransform.transform(Objects.requireNonNull(resultEntry.getVector())),
                                                0.0d), 10));
        clusterNeighborhood.thenCompose(clusterMetadatas -> {
            //
            // Not having the primary cluster in the neighborhood should be next to impossible. It can happen, however,
            // and we need to build for that rare corner case. Here we look for the primary cluster in the cluster
            // neighborhood and adjust the inner and outer neighborhood accordingly. Also log, if we cannot find the
            // primary cluster as that should be almost indicative of another problem.
            //
            boolean foundPrimaryCluster = false;
            for (final ClusterMetadataWithDistance clusterMetadata : clusterMetadatas) {
                if (clusterMetadata.getClusterMetadata().getId().equals(primaryClusterMetadata.getId())) {
                    foundPrimaryCluster = true;
                    break;
                }
            }

            final List<ClusterMetadataWithDistance> innerNeighborhood;
            final List<ClusterMetadataWithDistance> outerNeighborhood;
            if (foundPrimaryCluster) {
                innerNeighborhood = clusterMetadatas.subList(0, numInnerNeighborhood);
                outerNeighborhood = clusterMetadatas.subList(numInnerNeighborhood, clusterMetadatas.size());
            } else {
                final ImmutableList.Builder<ClusterMetadataWithDistance> innerNeighborhoodBuilder = ImmutableList.builder();
                innerNeighborhoodBuilder.add(
                        new ClusterMetadataWithDistance(primaryClusterMetadata,
                                storageTransform.transform(primaryClusterCentroid), 0.0d));
                innerNeighborhoodBuilder.addAll(clusterMetadatas.subList(0, numInnerNeighborhood - 1));
                innerNeighborhood = innerNeighborhoodBuilder.build();
                outerNeighborhood = clusterMetadatas.subList(numInnerNeighborhood - 1, clusterMetadatas.size() - 1);
            }

            //
            // At this point innerNeighborhood contains the clusters we want to split into
            // innerNeighborhood.size() + 1 number of clusters and outerNeighborhood contains all clusters we
            // may assign some vectors from innerNeighborhood to.
            //

            return forEach(innerNeighborhood,
                    clusterMetadata ->
                            primitives.fetchCluster(transaction, storageTransform,
                                    clusterMetadata.getClusterMetadata().getId(), clusterMetadata.getCentroid()),
                    10,
                    executor)
                    .thenCompose(clusters -> {
                        final ImmutableList.Builder<VectorReference> vectorsBuilder = ImmutableList.builder();
                        for (final Cluster cluster : clusters) {
                            vectorsBuilder.addAll(cluster.getVectorReferences());
                        }

                        final int k = innerNeighborhood.size() + 1;
                        final ImmutableList<VectorReference> vectorReferences = vectorsBuilder.build();
                        final BoundedKMeans.Result<Transformed<RealVector>> kMeansResult =
                                BoundedKMeans.fit(random, estimator, vectorReferenceVectorLens, Transformed.underlyingLens(),
                                        vectorReferences, k, 3, 1, 0.05,
                                        BoundedKMeans.overflowQuadraticPenalty(), true);
                        Verify.verify(kMeansResult.getClusterCentroids().size() == k);

                        final ImmutableList.Builder<UUID> newClusterIdsBuilder = ImmutableList.builder();
                        for (int i = 0; i < k; i ++) {
                            newClusterIdsBuilder.add(UUID.randomUUID());
                        }
                        final List<UUID> newClusterIds = newClusterIdsBuilder.build();

                        final ImmutableListMultimap.Builder<UUID, VectorReference>

                        return AsyncUtil.DONE;
                    });
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

    /**
     * Lens to access the underlying vector of a transformed vector in logic that can be called for containers of
     * both vectors and transformed vectors.
     */
    private static class VectorReferenceVectorLens implements Lens<VectorReference, Transformed<RealVector>> {
        @Nullable
        @Override
        public Transformed<RealVector> get(@Nonnull final VectorReference vectorReference) {
            return vectorReference.getVector();
        }

        @Nonnull
        @Override
        public VectorReference set(@Nullable final VectorReference vectorReference,
                                   @Nullable final Transformed<RealVector> transformed) {
            Objects.requireNonNull(vectorReference);
            return new VectorReference(vectorReference.getId(), vectorReference.isPrimaryCopy(),
                    Objects.requireNonNull(transformed));
        }
    }

    private static class VectorAssignment {
        @Nonnull
        private final Transformed<RealVector> vector;
        @Nonnull
        private final UUID clusterId;
        private final double distanceToClusterCentroid;

        public VectorAssignment(@Nonnull final Transformed<RealVector> vector,
                                @Nonnull final UUID clusterId,
                                final double distanceToClusterCentroid) {
            this.vector = vector;
            this.clusterId = clusterId;
            this.distanceToClusterCentroid = distanceToClusterCentroid;
        }

        @Nonnull
        public Transformed<RealVector> getVector() {
            return vector;
        }

        @Nonnull
        public UUID getClusterId() {
            return clusterId;
        }

        public double getDistanceToClusterCentroid() {
            return distanceToClusterCentroid;
        }
    }
}
