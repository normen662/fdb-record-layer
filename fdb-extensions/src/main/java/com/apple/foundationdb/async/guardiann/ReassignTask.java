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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class ReassignTask extends AbstractDeferredTask {
    private ReassignTask(@Nonnull final UUID taskId) {
        super(taskId);
    }

    @Nonnull
    @Override
    public Tuple valueTuple() {
        return Tuple.from(getKind().getCode());
    }

    @Nonnull
    public CompletableFuture<Void> runTask() {
        return AsyncUtil.DONE;
    }

    @Nonnull
    public Kind getKind() {
        return Kind.REASSIGN;
    }

    @Nonnull
    public static ReassignTask fromTuples(@Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        Verify.verify(Kind.fromValueTuple(valueTuple) == Kind.SPLIT_MERGE);
        return new ReassignTask(keyTuple.getUUID(0));
    }
}
