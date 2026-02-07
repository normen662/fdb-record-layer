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

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.UUID;

class ClusterState {
    @Nonnull
    private final UUID uuid;
    private final boolean isDraining;

    public ClusterState(@Nonnull final UUID uuid, final boolean isDraining) {
        this.uuid = uuid;
        this.isDraining = isDraining;
    }

    @Nonnull
    public UUID getUuid() {
        return uuid;
    }

    public boolean isDraining() {
        return isDraining;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClusterState cluster = (ClusterState)o;
        return Objects.equals(getUuid(), cluster.getUuid()) &&
                isDraining() == cluster.isDraining();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getUuid(), isDraining());
    }
}
