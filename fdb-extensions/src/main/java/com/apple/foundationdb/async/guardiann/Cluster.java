/*
 * Cluster.java
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

import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

class Cluster {
    @Nonnull
    private final UUID uuid;
    @Nonnull
    private final Transformed<RealVector> centroid;
    private final boolean isDraining;
    @Nonnull
    private final List<VectorReferences> vectorEntries;

    public Cluster(@Nonnull final UUID uuid, @Nonnull final Transformed<RealVector> centroid,
                   final boolean isDraining, @Nonnull final List<VectorReferences> vectorEntries) {
        this.uuid = uuid;
        this.centroid = centroid;
        this.isDraining = isDraining;
        this.vectorEntries = vectorEntries;
    }

    @Nonnull
    public UUID getUuid() {
        return uuid;
    }

    @Nonnull
    public Transformed<RealVector> getCentroid() {
        return centroid;
    }

    public boolean isDraining() {
        return isDraining;
    }

    @Nonnull
    public List<VectorReferences> getVectorEntries() {
        return vectorEntries;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Cluster cluster = (Cluster)o;
        return Objects.equals(getUuid(), cluster.getUuid()) &&
                Objects.equals(getCentroid(), cluster.getCentroid()) &&
                Objects.equals(getVectorEntries(), cluster.getVectorEntries());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getUuid(), getCentroid(), getVectorEntries());
    }
}
