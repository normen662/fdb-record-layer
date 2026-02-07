/*
 * Insert.java
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
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.common.AggregatedVector;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.FhtKacRotator;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.rabitq.RaBitQuantizer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.ToDoubleFunction;

import static com.apple.foundationdb.async.MoreAsyncUtil.forLoop;
import static com.apple.foundationdb.async.common.StorageHelpers.aggregateVectors;
import static com.apple.foundationdb.async.common.StorageHelpers.appendSampledVector;
import static com.apple.foundationdb.async.common.StorageHelpers.consumeSampledVectors;
import static com.apple.foundationdb.async.common.StorageHelpers.deleteAllSampledVectors;

/**
 * TODO.
 */
@API(API.Status.EXPERIMENTAL)
public class Insert {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(Insert.class);

    @Nonnull
    private final Locator locator;

    /**
     * This constructor initializes a new insert operations object with the necessary components for storage,
     * execution, configuration, and event handling.
     *
     * @param locator the {@link Locator} where the graph data is stored, which config to use, which executor to use,
     *        etc.
     */
    public Insert(@Nonnull final Locator locator) {
        this.locator = locator;
    }

    @Nonnull
    public Locator getLocator() {
        return locator;
    }

    /**
     * Gets the subspace associated with this object.
     *
     * @return the non-null subspace
     */
    @Nonnull
    public Subspace getSubspace() {
        return getLocator().getSubspace();
    }

    /**
     * Get the executor used by this hnsw.
     * @return executor used when running asynchronous tasks
     */
    @Nonnull
    public Executor getExecutor() {
        return getLocator().getExecutor();
    }

    /**
     * Get the configuration of this hnsw.
     * @return hnsw configuration
     */
    @Nonnull
    public Config getConfig() {
        return getLocator().getConfig();
    }

    /**
     * Get the on-write listener.
     * @return the on-write listener
     */
    @Nonnull
    public OnWriteListener getOnWriteListener() {
        return getLocator().getOnWriteListener();
    }

    /**
     * Get the on-read listener.
     * @return the on-read listener
     */
    @Nonnull
    public OnReadListener getOnReadListener() {
        return getLocator().getOnReadListener();
    }

    @Nonnull
    private Primitives primitives() {
        return getLocator().primitives();
    }

    @Nonnull
    private Search searcher() {
        return getLocator().search();
    }

    @Nonnull
    private StorageAdapter getStorageAdapter() {
        return getLocator().getStorageAdapter();
    }

    @Nonnull
    private Subspace getAccessInfoSubspace() {
        return getStorageAdapter().getAccessInfoSubspace();
    }

    @Nonnull
    private Subspace getClusterHnswSubspace() {
        return getStorageAdapter().getClusterHnswSubspace();
    }

    @Nonnull
    private Subspace getClusterStatesSubspace() {
        return getStorageAdapter().getClusterStatesSubspace();
    }

    @Nonnull
    private Subspace getVectorReferencesSubspace() {
        return getStorageAdapter().getVectorReferencesSubspace();
    }

    @Nonnull
    private Subspace getSamplesSubspace() {
        return getStorageAdapter().getSamplesSubspace();
    }

    /**
     * Inserts a new vector with its associated primary key into the HNSW graph.
     * <p>
     * The method first determines a layer for the new node, called the {@code top layer}.
     * It then traverses the graph from the entry point downwards, greedily searching for the nearest
     * neighbors to the {@code newVector} at each layer. This search identifies the optimal
     * connection points for the new node.
     * <p>
     * Once the nearest neighbors are found, the new node is linked into the graph structure at all
     * layers up to its {@code top layer}. Special handling is included for inserting the
     * first-ever node into the graph or when a new node's layer is higher than any existing node,
     * which updates the graph's entry point. All operations are performed asynchronously.
     *
     * @param transaction the {@link Transaction} context for all database operations
     * @param newPrimaryKey the unique {@link Tuple} primary key for the new node being inserted
     * @param newVector the {@link RealVector} data to be inserted into the graph
     * @param newAdditionalValues additional values that are associated with the new vector and stored with the node
     *
     * @return a {@link CompletableFuture} that completes when the insertion operation is finished
     */
    @Nonnull
    public CompletableFuture<Void> insert(@Nonnull final Transaction transaction, @Nonnull final Tuple newPrimaryKey,
                                          @Nonnull final RealVector newVector,
                                          @Nullable final Tuple newAdditionalValues) {
        final Primitives primitives = primitives();

        return primitives.fetchAccessInfo(transaction)
                .thenCombine(primitives.exists(transaction, newPrimaryKey),
                        (accessInfo, nodeAlreadyExists) -> {
                            if (nodeAlreadyExists) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("new record already exists in HNSW with key={} on layer={}",
                                            newPrimaryKey, insertionLayer);
                                }
                            }
                            return new Primitives.AccessInfoAndNodeExistence(accessInfo, nodeAlreadyExists);
                        })
                .thenCompose(accessInfoAndNodeExistence -> {
                    if (accessInfoAndNodeExistence.isNodeExists()) {
                        return AsyncUtil.DONE;
                    }

                    final AccessInfo accessInfo = accessInfoAndNodeExistence.getAccessInfo();
                    if (accessInfo == null) {
                        firstInsert(transaction, newPrimaryKey, newVector, newAdditionalValues, random, primitives,
                                insertionLayer);
                        return AsyncUtil.DONE;
                    }

                    final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                    final Transformed<RealVector> transformedNewVector = storageTransform.transform(newVector);
                    final Quantizer quantizer = primitives.quantizer(accessInfo);
                    final Estimator estimator = quantizer.estimator();

                    final AccessInfo currentAccessInfo;

                    final EntryNodeReference entryNodeReference = accessInfo.getEntryNodeReference();
                    final int lMax = entryNodeReference.getLayer();
                    if (logger.isTraceEnabled()) {
                        logger.trace("entry node read with key {} at layer {}", entryNodeReference.getPrimaryKey(), lMax);
                    }

                    if (insertionLayer > lMax) {
                        primitives.writeLonelyNodes(quantizer, transaction, newPrimaryKey, transformedNewVector,
                                newAdditionalValues, insertionLayer, lMax);
                        currentAccessInfo = accessInfo.withNewEntryNodeReference(
                                new EntryNodeReference(newPrimaryKey, transformedNewVector,
                                        insertionLayer));
                        com.apple.foundationdb.async.hnsw.StorageAdapter.writeAccessInfo(transaction, getSubspace(), currentAccessInfo,
                                getOnWriteListener());
                        if (logger.isTraceEnabled()) {
                            logger.trace("written higher entry node reference with key={} on layer={}",
                                    newPrimaryKey, insertionLayer);
                        }
                    } else {
                        currentAccessInfo = accessInfo;
                    }

                    final ToDoubleFunction<Transformed<RealVector>> objectiveFunction =
                            Search.distanceToTargetVector(estimator, transformedNewVector);
                    final NodeReferenceWithDistance initialNodeReference =
                            new NodeReferenceWithDistance(entryNodeReference.getPrimaryKey(),
                                    entryNodeReference.getVector(),
                                    objectiveFunction.applyAsDouble(entryNodeReference.getVector()));
                    final Search search = searcher();

                    return forLoop(lMax, initialNodeReference,
                            layer -> layer > insertionLayer,
                            layer -> layer - 1,
                            (layer, previousNodeReference) -> {
                                final com.apple.foundationdb.async.hnsw.StorageAdapter<? extends NodeReference> storageAdapter =
                                        primitives.storageAdapterForLayer(layer);
                                return search.greedySearchLayer(storageAdapter, transaction, storageTransform,
                                        previousNodeReference, layer, objectiveFunction);
                            }, getExecutor())
                            .thenCompose(nodeReference ->
                                    insertIntoLayers(transaction, storageTransform, quantizer, newPrimaryKey,
                                            transformedNewVector, newAdditionalValues, nodeReference, lMax,
                                            insertionLayer))
                            .thenCompose(ignored ->
                                    addToStatsIfNecessary(random, transaction, currentAccessInfo, transformedNewVector));
                }).thenCompose(ignored -> AsyncUtil.DONE);
    }

    private void firstInsert(@Nonnull final Transaction transaction,
                             @Nonnull final Tuple newPrimaryKey,
                             @Nonnull final RealVector newVector,
                             @Nullable final Tuple additionalValues,
                             @Nonnull final SplittableRandom random,
                             @Nonnull final Primitives primitives,
                             final int insertionLayer) {
        final com.apple.foundationdb.async.hnsw.Config config = getConfig();
        final long rotatorSeed;
        final StorageTransform storageTransform;
        final Quantizer quantizer;
        final RealVector negatedCentroid;
        if (config.isUseRaBitQ() &&
                !config.getMetric().satisfiesPreservedUnderTranslation()) {
            //
            // The metric does not preserve distances under translation of the vectors, but we are supposed to encode
            // the vectors using RaBitQ. There is no point in sampling the centroid as we cannot translate any vectors.
            // Instead, we use RaBitQ immediately under an identity translation.
            //
            rotatorSeed = random.nextLong();
            storageTransform = primitives.storageTransform(rotatorSeed, null,
                    primitives.isMetricNeedsNormalizedVectors());
            quantizer = new RaBitQuantizer(config.getMetric(), config.getRaBitQNumExBits());
            negatedCentroid = DoubleRealVector.zeroVector(config.getNumDimensions());
        } else {
            rotatorSeed = -1L;
            storageTransform = StorageTransform.identity();
            quantizer = Quantizer.noOpQuantizer(config.getMetric());
            negatedCentroid = null;
        }

        final Transformed<RealVector> transformedNewVector = storageTransform.transform(newVector);

        // this is the first node
        primitives.writeLonelyNodes(quantizer, transaction, newPrimaryKey, transformedNewVector, additionalValues,
                insertionLayer, -1);
        final AccessInfo initialAccessInfo = new AccessInfo(
                new EntryNodeReference(newPrimaryKey, transformedNewVector, insertionLayer),
                rotatorSeed, negatedCentroid);
        writeAccessInfo(transaction, getSubspace(), initialAccessInfo,
                getOnWriteListener());
        if (logger.isTraceEnabled()) {
            logger.trace("written initial entry node reference with key={} on layer={}",
                    newPrimaryKey, insertionLayer);
        }
    }

    /**
     * Method to keep stats if necessary. Stats need to be kept and maintained when the client would like to use
     * e.g. RaBitQ as RaBitQ needs a stable somewhat correct centroid in order to function properly.
     * <p>
     * Specifically for RaBitQ, we add vectors to a set of sampled vectors in a designated subspace of the HNSW
     * structure. The parameter {@link Config#getSampleVectorStatsProbability()} governs when we do sample. Another
     * parameter, {@link Config#getMaintainStatsProbability()}, determines how many times we add-up/replace (consume)
     * vectors from this sampled-vector space and aggregate them in the typical running count/running sum scheme
     * in order to finally compute the centroid if {@link Config#getStatsThreshold()} number of vectors have been
     * sampled and aggregated. That centroid is then used to update the access info.
     *
     * @param random a random to use
     * @param transaction the transaction
     * @param currentAccessInfo this current access info that was fetched as part of an insert
     * @param transformedNewVector the new vector (in the transformed coordinate system) that may be added
     * @return a future that returns {@code null} when completed
     */
    @Nonnull
    private CompletableFuture<Void> addToStatsIfNecessary(@Nonnull final SplittableRandom random,
                                                          @Nonnull final Transaction transaction,
                                                          @Nonnull final AccessInfo currentAccessInfo,
                                                          @Nonnull final Transformed<RealVector> transformedNewVector) {
        final Subspace samplesSubspace = getSamplesSubspace();
        if (getConfig().isUseRaBitQ() &&
                !currentAccessInfo.canUseRaBitQ()) {
            final Primitives primitives = primitives();
            if (shouldSampleVector(random)) {
                appendSampledVector(transaction, samplesSubspace, 1, transformedNewVector,
                        getOnWriteListener());
            }
            if (shouldMaintainStats(random)) {
                return consumeSampledVectors(transaction, samplesSubspace,
                                50, getOnReadListener())
                        .thenApply(sampledVectors -> {
                            final AggregatedVector aggregatedSampledVector =
                                    aggregateVectors(sampledVectors);

                            if (aggregatedSampledVector != null) {
                                final int partialCount = aggregatedSampledVector.getPartialCount();
                                final Transformed<RealVector> partialVector = aggregatedSampledVector.getPartialVector();
                                appendSampledVector(transaction, samplesSubspace, partialCount, partialVector,
                                        getOnWriteListener());
                                if (logger.isTraceEnabled()) {
                                    logger.trace("updated stats with numVectors={}, partialCount={}, partialVector={}",
                                            sampledVectors.size(), partialCount, partialVector);
                                }

                                if (partialCount >= getConfig().getStatsThreshold()) {
                                    final long rotatorSeed = random.nextLong();
                                    final FhtKacRotator rotator =
                                            new FhtKacRotator(rotatorSeed, getConfig().getNumDimensions(), 10);

                                    final Transformed<RealVector> centroid =
                                            partialVector.multiply(-1.0d / partialCount);
                                    final RealVector rotatedCentroid =
                                            rotator.apply(centroid.getUnderlyingVector());
                                    final StorageTransform storageTransform =
                                            new StorageTransform(rotator, rotatedCentroid,
                                                    primitives().isMetricNeedsNormalizedVectors());

                                    final AccessInfo newAccessInfo =
                                            new AccessInfo(rotatorSeed, rotatedCentroid);
                                    primitives.writeAccessInfo(transaction, newAccessInfo);
                                    deleteAllSampledVectors(transaction, samplesSubspace, getOnWriteListener());
                                    if (logger.isTraceEnabled()) {
                                        logger.trace("established rotatorSeed={}, centroid with count={}, centroid={}",
                                                rotatorSeed, partialCount, rotatedCentroid);
                                    }
                                }
                            }
                            return null;
                        });
            }
        }
        return AsyncUtil.DONE;
    }

    private boolean shouldSampleVector(@Nonnull final SplittableRandom random) {
        return random.nextDouble() < getConfig().getSampleVectorStatsProbability();
    }

    private boolean shouldMaintainStats(@Nonnull final SplittableRandom random) {
        return random.nextDouble() < getConfig().getMaintainStatsProbability();
    }
}
