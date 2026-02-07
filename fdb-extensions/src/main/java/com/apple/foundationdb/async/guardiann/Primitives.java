/*
 * Primitives.java
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.FhtKacRotator;
import com.apple.foundationdb.linear.LinearOperator;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.rabitq.RaBitQuantizer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * An implementation of primitives for the Hierarchical Navigable Small World (HNSW) algorithm for
 * efficient approximate nearest neighbor (ANN) search.
 */
@API(API.Status.EXPERIMENTAL)
public class Primitives {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(Primitives.class);

    @Nonnull
    private final Locator locator;

    /**
     * Constructs a new primitives instance.
     *
     * @param locator the {@link Locator} where the graph data is stored, which config to use, which executor to use,
     *        etc.
     */
    public Primitives(@Nonnull final Locator locator) {
        this.locator = locator;
    }

    @Nonnull
    public Locator getLocator() {
        return locator;
    }

    @Nonnull
    StorageAdapter getStorageAdapter() {
        return locator.getStorageAdapter();
    }

    /**
     * Gets the subspace associated with this object.
     *
     * @return the non-null subspace
     */
    @Nonnull
    private Subspace getSubspace() {
        return getLocator().getSubspace();
    }

    /**
     * Get the executor used by this hnsw.
     * @return executor used when running asynchronous tasks
     */
    @Nonnull
    private Executor getExecutor() {
        return getLocator().getExecutor();
    }

    /**
     * Get the configuration.
     * @return the configuration
     */
    @Nonnull
    private Config getConfig() {
        return getLocator().getConfig();
    }

    /**
     * Get the on-write listener.
     * @return the on-write listener
     */
    @Nonnull
    private OnWriteListener getOnWriteListener() {
        return getLocator().getOnWriteListener();
    }

    /**
     * Get the on-read listener.
     * @return the on-read listener
     */
    @Nonnull
    private OnReadListener getOnReadListener() {
        return getLocator().getOnReadListener();
    }

    @Nonnull
    Subspace getAccessInfoSubspace() {
        return getStorageAdapter().getAccessInfoSubspace();
    }

    @Nonnull
    Subspace getClusterHnswSubspace() {
        return getStorageAdapter().getClusterHnswSubspace();
    }

    @Nonnull
    Subspace getClusterStatesSubspace() {
        return getStorageAdapter().getClusterStatesSubspace();
    }

    @Nonnull
    Subspace getVectorReferencesSubspace() {
        return getStorageAdapter().getVectorReferencesSubspace();
    }

    @Nonnull
    Subspace getSamplesSubspace() {
        return getStorageAdapter().getSamplesSubspace();
    }

    boolean isMetricNeedsNormalizedVectors() {
        return getConfig().getMetric() == Metric.COSINE_METRIC;
    }

    @Nonnull
    StorageTransform storageTransform(@Nullable final AccessInfo accessInfo) {
        if (accessInfo == null || !accessInfo.canUseRaBitQ()) {
            return StorageTransform.identity();
        }

        return storageTransform(accessInfo.getRotatorSeed(),
                Objects.requireNonNull(accessInfo.getNegatedCentroid()),
                isMetricNeedsNormalizedVectors());
    }

    @Nonnull
    StorageTransform storageTransform(@Nullable final Long rotatorSeed,
                                      @Nullable final RealVector negatedCentroid,
                                      final boolean normalizeVectors) {
        final LinearOperator linearOperator =
                rotatorSeed == null
                ? null : new FhtKacRotator(rotatorSeed, getConfig().getNumDimensions(), 10);

        return new StorageTransform(linearOperator, negatedCentroid, normalizeVectors);
    }

    @Nonnull
    Quantizer quantizer(@Nullable final AccessInfo accessInfo) {
        if (accessInfo == null || !accessInfo.canUseRaBitQ()) {
            return Quantizer.noOpQuantizer(getConfig().getMetric());
        }

        final Config config = getConfig();
        return config.isUseRaBitQ()
               ? new RaBitQuantizer(config.getMetric(), config.getRaBitQNumExBits())
               : Quantizer.noOpQuantizer(config.getMetric());
    }

    @Nonnull
    CompletableFuture<AccessInfo> fetchAccessInfo(@Nonnull final ReadTransaction readTransaction) {
        final Subspace accessInfoSubspace = getAccessInfoSubspace();
        final byte[] key = accessInfoSubspace.pack();

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(-1, key, valueBytes);
                    if (valueBytes == null) {
                        return null; // not a single node in the index
                    }
                    return StorageAdapter.accessInfoFromTuple(getConfig(), Tuple.fromBytes(valueBytes));
                });
    }

    void writeAccessInfo(@Nonnull final Transaction transaction,
                         @Nonnull final AccessInfo accessInfo) {
        final Subspace accessInfoSubspace = getAccessInfoSubspace();
        final byte[] key = accessInfoSubspace.pack();
        final byte[] value = StorageAdapter.tupleFromAccessInfo(accessInfo).pack();
        transaction.set(key, value);
        getOnWriteListener().onKeyValueWritten(-1, key, value);
    }

    @Nonnull
    CompletableFuture<Cluster> fetchCluster(@Nonnull final ReadTransaction readTransaction,
                                            @Nonnull final StorageTransform storageTransform,
                                            @Nonnull final UUID clusterUuid,
                                            @Nonnull final RealVector centroid) {
        final Transformed<RealVector> transformedCentroid = storageTransform.transform(centroid);
        return fetchCluster(readTransaction, storageTransform, clusterUuid, transformedCentroid);
    }

    @Nonnull
    CompletableFuture<Cluster> fetchCluster(@Nonnull final ReadTransaction readTransaction,
                                            @Nonnull final StorageTransform storageTransform,
                                            @Nonnull final UUID clusterUuid,
                                            @Nonnull final Transformed<RealVector> centroid) {
        return fetchClusterState(readTransaction, clusterUuid)
                .thenCombine(fetchVectorReferences(readTransaction, storageTransform, clusterUuid),
                        (clusterState, vectorReferences) ->
                                new Cluster(clusterUuid, centroid, clusterState.isDraining(), vectorReferences));
    }

    @Nonnull
    CompletableFuture<ClusterState> fetchClusterState(@Nonnull final ReadTransaction readTransaction,
                                                      @Nonnull final UUID clusterUuid) {
        final byte[] key = getClusterStatesSubspace().pack(Tuple.from(clusterUuid));
        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    getOnReadListener().onKeyValueRead(-1, key, valueBytes);
                    if (valueBytes == null) {
                        return null;
                    }
                    return StorageAdapter.clusterStateFromTuple(Tuple.fromBytes(valueBytes));
                });
    }

    @Nonnull
    CompletableFuture<List<VectorReference>> fetchVectorReferences(@Nonnull final ReadTransaction readTransaction,
                                                                   @Nonnull final StorageTransform storageTransform,
                                                                   @Nonnull final UUID clusterUuid) {
        final Subspace vectorReferencesSubspace = getVectorReferencesSubspace();
        final byte[] rangeKey = vectorReferencesSubspace.pack(Tuple.from(clusterUuid));

        return AsyncUtil.collect(readTransaction.getRange(Range.startsWith(rangeKey),
                        ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL), readTransaction.getExecutor())
                .thenApply(keyValues -> {
                    final ImmutableList.Builder<VectorReference> vectorReferencesBuilder = ImmutableList.builder();
                    for (final KeyValue keyValue : keyValues) {
                        final Tuple primaryKey = vectorReferencesSubspace.unpack(keyValue.getKey()).getNestedTuple(1);
                        final byte[] keyBytes = keyValue.getKey();
                        final byte[] valueBytes = keyValue.getValue();
                        vectorReferencesBuilder.add(
                                StorageAdapter.vectorReferenceFromTuples(getConfig(), storageTransform,
                                        primaryKey, Tuple.fromBytes(valueBytes)));
                        getOnReadListener().onKeyValueRead(-1, keyBytes, valueBytes);
                    }
                    return vectorReferencesBuilder.build();
                });
    }

    static class AccessInfoAndNodeExistence {
        @Nullable
        private final AccessInfo accessInfo;
        private final boolean nodeExists;

        public AccessInfoAndNodeExistence(@Nullable final AccessInfo accessInfo, final boolean nodeExists) {
            this.accessInfo = accessInfo;
            this.nodeExists = nodeExists;
        }

        @Nullable
        public AccessInfo getAccessInfo() {
            return accessInfo;
        }

        public boolean isNodeExists() {
            return nodeExists;
        }
    }
}
