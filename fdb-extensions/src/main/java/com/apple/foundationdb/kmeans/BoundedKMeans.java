/*
 * BoundedKMeans.java
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

package com.apple.foundationdb.kmeans;

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.MutableDoubleRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;

/**
 * Bounded, restartable Lloyd-style k-means intended for LOCAL cluster restructuring (SPFresh-style).
 * <p>
 * Drop-in replacement for the earlier BoundedKMeansGeneric:
 * - Adds OPTIONAL soft size balancing via lambda + SizePenalty
 *   (pass lambda=0 or sizePenalty=null to disable and get the old behavior)
 */
public final class BoundedKMeans {
    private BoundedKMeans() {
        // nothing
    }

    public static final class Result {
        public final int[] assignment;
        public final List<RealVector> centroids;
        public final int[] clusterSizes;
        public final double objective;

        public Result(@Nonnull final int[] assignment,
                      @Nonnull final List<RealVector> centroids,
                      @Nonnull final int[] clusterSizes,
                      final double objective) {
            this.assignment = assignment;
            this.centroids = centroids;
            this.clusterSizes = clusterSizes;
            this.objective = objective;
        }
    }

    /**
     * Soft balancing penalty hook.
     * <p>
     * Returned value is multiplied by lambda and added to the distance during assignment.
     * Use lambda=0 or sizePenalty=null to disable.
     */
    @FunctionalInterface
    public interface SizePenalty {
        double penalty(int projectedSize, int targetSize);
    }

    /** Reasonable default: penalize only overflow above target size (quadratic). */
    public static SizePenalty overflowQuadraticPenalty() {
        return (proj, target) -> {
            int overflow = Math.max(0, proj - target);
            return (double) overflow * (double) overflow / Math.max(1, target);
        };
    }

    /**
     * /**
     * TODO.
     * Shuffling reduces order-dependence when using projected-size penalties and usually improves balance quality.
     *
     * @param lambda strength of size balancing; 0 disables
     * @param sizePenalty penalty function; null disables
     */
    public static Result fit(@Nonnull final SplittableRandom random, @Nonnull final Estimator estimator,
                             @Nonnull final List<DoubleRealVector> vectors, final int k, final int maxIterations,
                             final int restarts, final double lambda, @Nullable final SizePenalty sizePenalty,
                             final boolean shuffleEachIteration) {

        Preconditions.checkArgument(k >= 2, "k must be >= 2");
        Preconditions.checkArgument(vectors.size() >= k, "vectors.size() must be >= k");
        Preconditions.checkArgument(maxIterations >= 1, "maxIterations must be >= 1");
        Preconditions.checkArgument(restarts >= 0, "restarts must be >= 0");
        Preconditions.checkArgument(lambda >= 0, "lambda must be >= 0");

        final int numDimensions = vectors.get(0).getNumDimensions();
        final int n = vectors.size();
        final int targetSize = Math.max(1, n / k);

        // Pre-build an index array so we can shuffle without moving data.
        final int[] order = new int[n];
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }

        Result best = null;

        for (int r = 0; r <= restarts; r++) {
            List<MutableDoubleRealVector> centroids = initKMeansPP(random, vectors, k, estimator);
            final int[] assignment = new int[n];
            Arrays.fill(assignment, -1);
            final int[] clusterSizes = new int[k];

            for (int iter = 0; iter < maxIterations; iter++) {
                if (shuffleEachIteration) {
                    shuffleInPlace(random, order);
                }

                int changed = 0;

                //
                // Projected sizes updated online during assignment to implement size bias.
                // If balancing is disabled, this behaves like normal clusterSizes accumulation.
                //
                final int[] projected = new int[k];

                // assignment step
                for (int t = 0; t < n; t++) {
                    int i = order[t];
                    final DoubleRealVector vector = vectors.get(i);

                    int bestC = 0;
                    double bestScore = score(estimator, vector, 0, centroids, projected, targetSize,
                            lambda, sizePenalty);

                    for (int c = 1; c < k; c++) {
                        double s = score(estimator, vector, c, centroids, projected, targetSize, lambda, sizePenalty);
                        if (s < bestScore) {
                            bestScore = s;
                            bestC = c;
                        }
                    }

                    if (assignment[i] != bestC) {
                        assignment[i] = bestC;
                        changed++;
                    }

                    projected[bestC]++; // commit this vector to the projected size
                }

                // Use projected sizes as current sizes
                System.arraycopy(projected, 0, clusterSizes, 0, k);

                if (changed == 0) {
                    break;
                }

                // Update step
                final List<MutableDoubleRealVector> newCentroids = new ArrayList<>(k);
                for (int c = 0; c < k; c++) {
                    newCentroids.add(MutableDoubleRealVector.zeroVector(numDimensions));
                }

                for (int i = 0; i < n; i++) {
                    newCentroids.get(assignment[i]).add(vectors.get(i));
                }

                for (int c = 0; c < k; c++) {
                    if (clusterSizes[c] == 0) {
                        // Reseed empty cluster with the "hardest" point under current centroids.
                        int idx = farthestPointIndex(vectors, centroids, estimator);
                        newCentroids.set(c, vectors.get(idx).toMutable());
                        clusterSizes[c] = 1; // used only to avoid div-by-zero; next iter recomputes sizes
                    } else {
                        newCentroids.get(c).multiply(1.0d / clusterSizes[c]);
                    }
                }

                centroids = newCentroids;
            }

            // Objective: sum of distances (NOT including size penalty) to compare restarts fairly.
            double objective = 0.0;
            for (int i = 0; i < n; i++) {
                objective += estimator.distance(vectors.get(i), centroids.get(assignment[i]));
            }

            final List<RealVector> centroidCopies = new ArrayList<>(k);
            for (final MutableDoubleRealVector centroid : centroids) {
                centroidCopies.add(centroid.toImmutable());
            }

            final Result candidate =
                    new Result(assignment.clone(), centroidCopies, clusterSizes.clone(), objective);

            if (best == null || candidate.objective < best.objective) {
                best = candidate;
            }
        }

        return best;
    }

    private static double score(@Nonnull final Estimator estimator, @Nonnull final RealVector vector,
                                final int c, @Nonnull final List<MutableDoubleRealVector> centroids,
                                final int[] projectedSizes, final int targetSize, final double lambda,
                                @Nullable final SizePenalty sizePenalty) {
        double d = estimator.distance(vector, centroids.get(c));
        if (lambda == 0.0 || sizePenalty == null) {
            return d;
        }

        int proj = projectedSizes[c] + 1;
        return d + lambda * sizePenalty.penalty(proj, targetSize);
    }

    /**
     * k-means++ initialization: pick first centroid at random, then sample next centroids
     * with probability proportional to squared distance to nearest chosen centroid.
     */
    private static List<MutableDoubleRealVector> initKMeansPP(@Nonnull final SplittableRandom random,
                                                              @Nonnull final List<DoubleRealVector> vectors,
                                                              final int k,
                                                              @Nonnull final Estimator estimator) {
        int n = vectors.size();

        List<MutableDoubleRealVector> centroids = new ArrayList<>(k);
        centroids.add(vectors.get(random.nextInt(n)).toMutable());

        double[] weights = new double[n];

        while (centroids.size() < k) {
            double total = 0.0;

            for (int i = 0; i < n; i++) {
                final DoubleRealVector vector = vectors.get(i);
                double minD = Double.MAX_VALUE;
                for (final MutableDoubleRealVector centroid : centroids) {
                    final double d = estimator.distance(vector, centroid);
                    if (d < minD) {
                        minD = d;
                    }
                }

                double w = minD * minD;
                weights[i] = w;
                total += w;
            }

            if (total == 0.0) {
                centroids.add(vectors.get(random.nextInt(n)).toMutable());
                continue;
            }

            double pick = random.nextDouble() * total;
            double cum = 0.0d;
            int chosen = n - 1;

            for (int i = 0; i < n; i++) {
                cum += weights[i];
                if (cum >= pick) {
                    chosen = i;
                    break;
                }
            }

            centroids.add(vectors.get(chosen).toMutable());
        }
        return centroids;
    }

    /**
     * Index of point whose minimum distance to any centroid is maximal.
     * Used to reseed empty clusters.
     */
    private static int farthestPointIndex(@Nonnull final List<? extends RealVector> vectors,
                                          @Nonnull final List<MutableDoubleRealVector> centroids,
                                          @Nonnull final Estimator estimator) {
        double best = -1.0;
        int bestIdx = 0;

        for (int i = 0; i < vectors.size(); i++) {
            RealVector vector = vectors.get(i);

            double min = Double.MAX_VALUE;
            for (final MutableDoubleRealVector centroid : centroids) {
                double d = estimator.distance(vector, centroid);
                if (d < min) {
                    min = d;
                }
            }

            if (min > best) {
                best = min;
                bestIdx = i;
            }
        }

        return bestIdx;
    }

    /** Fisher–Yates shuffle of an int[] using ThreadLocalRandom. */
    private static void shuffleInPlace(@Nonnull final SplittableRandom random, @Nonnull final int[] a) {
        for (int i = a.length - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            int tmp = a[i];
            a[i] = a[j];
            a[j] = tmp;
        }
    }
}
